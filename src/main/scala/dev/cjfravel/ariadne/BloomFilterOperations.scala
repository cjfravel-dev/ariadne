package dev.cjfravel.ariadne

import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/** Trait providing bloom filter operations for Index instances.
  *
  * This trait handles:
  * - Building bloom filters during index updates
  * - Serializing/deserializing bloom filters for Delta storage
  * - Querying bloom filters for file location
  *
  * Bloom filters are probabilistic data structures that provide:
  * - Guaranteed NO false negatives (if filter says "no", value definitely absent)
  * - Configurable false positive rate (if filter says "yes", value MIGHT be present)
  * - Space-efficient storage (approximately 10 bits per element at 1% FPR)
  */
trait BloomFilterOperations extends IndexFileOperations {
  self: Index =>

  /** Column name prefix for bloom filter storage */
  protected val bloomColumnPrefix = "bloom_"

  /** Get bloom index configurations from metadata */
  protected def bloomIndexConfigs: Seq[BloomIndexConfig] =
    metadata.bloom_indexes.asScala.toSeq

  /** Get the set of bloom index column names */
  protected def bloomColumns: Set[String] =
    metadata.bloom_indexes.asScala.map(_.column).toSet

  /** Get the set of bloom storage column names (with prefix) */
  protected def bloomStorageColumns: Set[String] =
    metadata.bloom_indexes.asScala.map(c => bloomColumnPrefix + c.column).toSet

  /** Builds bloom filter indexes for all configured bloom columns.
    *
    * @param df
    *   DataFrame with filename column and source data
    * @return
    *   DataFrame with bloom filter binary columns added
    */
  protected def buildBloomFilterIndexes(df: DataFrame): DataFrame = {
    val configs = bloomIndexConfigs
    if (configs.isEmpty) return spark.emptyDataFrame

    // For each bloom column, create aggregation that produces binary bloom filter
    configs.foldLeft(df.select("filename").distinct) { (accumDf, config) =>
      val bloomColumn = bloomColumnPrefix + config.column
      val fprValue = config.fpr  // Capture FPR as a local val

      // Get distinct values per file and create bloom filter
      val bloomUdf = createBloomFilterUdf(fprValue)
      val bloomData = df
        .select("filename", config.column)
        .groupBy("filename")
        .agg(
          bloomUdf(collect_set(col(config.column))).alias(bloomColumn)
        )

      accumDf.join(bloomData, Seq("filename"), "left")
    }
  }

  /** Creates a UDF that converts an array of values to a serialized bloom
    * filter.
    *
    * @param fprValue
    *   False positive rate (captured as a serializable value)
    * @return
    *   UDF that takes Array[Any] and returns Array[Byte]
    */
  private def createBloomFilterUdf(fprValue: Double): UserDefinedFunction = {
    // Capture the FPR as a local constant to ensure it's serializable
    val fpr: Double = fprValue
    
    udf { (values: Seq[Any]) =>
      if (values == null || values.isEmpty) {
        null
      } else {
        val nonNullValues = values.filter(_ != null)
        if (nonNullValues.isEmpty) {
          null
        } else {
          // Create bloom filter sized for the actual number of elements
          val expectedInsertions = math.max(nonNullValues.size, 1)
          val bf = BloomFilter.create(
            Funnels.stringFunnel(StandardCharsets.UTF_8),
            expectedInsertions.toLong,
            fpr
          )

          // Add all values (convert to string for universal hashing)
          nonNullValues.foreach(v => bf.put(v.toString))

          // Serialize to bytes
          val baos = new ByteArrayOutputStream()
          bf.writeTo(baos)
          baos.toByteArray
        }
      }
    }
  }

  /** Deserializes a bloom filter from bytes.
    *
    * @param bytes
    *   Serialized bloom filter
    * @return
    *   BloomFilter instance
    */
  protected def deserializeBloomFilter(
      bytes: Array[Byte]
  ): BloomFilter[CharSequence] = {
    val bais = new ByteArrayInputStream(bytes)
    BloomFilter.readFrom(bais, Funnels.stringFunnel(StandardCharsets.UTF_8))
  }

  /** Checks if a value might be contained in a serialized bloom filter.
    *
    * @param bloomBytes
    *   Serialized bloom filter
    * @param value
    *   Value to check
    * @return
    *   true if value MIGHT be present, false if DEFINITELY not present
    */
  protected def bloomMightContain(bloomBytes: Array[Byte], value: Any): Boolean = {
    if (bloomBytes == null || value == null) return false
    val bf = deserializeBloomFilter(bloomBytes)
    bf.mightContain(value.toString)
  }

  /** Creates a UDF for checking bloom filter membership.
    *
    * @return
    *   UDF that takes (bloom_bytes, value) and returns Boolean
    */
  protected def bloomContainsUdf: UserDefinedFunction = {
    udf { (bloomBytes: Array[Byte], value: String) =>
      if (bloomBytes == null || value == null) false
      else {
        val bf = deserializeBloomFilter(bloomBytes)
        bf.mightContain(value)
      }
    }
  }

  /** Locates files that might contain the given values using bloom filters.
    *
    * @param column
    *   The bloom index column name
    * @param values
    *   Values to search for
    * @param indexDf
    *   The index DataFrame to search
    * @return
    *   Set of filenames that might contain any of the values
    */
  protected def locateFilesWithBloom(
      column: String,
      values: Array[Any],
      indexDf: DataFrame
  ): Set[String] = {
    val bloomColumn = bloomColumnPrefix + column

    if (!indexDf.columns.contains(bloomColumn)) {
      logger.warn(s"Bloom column $bloomColumn not found in index")
      return Set.empty
    }

    // Check each file's bloom filter against all values
    val matchingFiles = indexDf
      .select("filename", bloomColumn)
      .collect()
      .filter { row =>
        val bloomBytes = row.getAs[Array[Byte]](bloomColumn)
        if (bloomBytes == null) false
        else {
          // File matches if ANY value might be contained
          values.exists(v => bloomMightContain(bloomBytes, v))
        }
      }
      .map(_.getString(0))
      .toSet

    matchingFiles
  }

  /** Locates files that might contain values from a DataFrame using bloom
    * filters.
    *
    * @param column
    *   The bloom index column name
    * @param valuesDf
    *   DataFrame containing values to search for
    * @param indexDf
    *   The index DataFrame to search
    * @return
    *   Set of filenames that might contain any of the values
    */
  protected def locateFilesWithBloomFromDataFrame(
      column: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Set[String] = {
    val bloomColumn = bloomColumnPrefix + column

    if (!indexDf.columns.contains(bloomColumn)) {
      logger.warn(s"Bloom column $bloomColumn not found in index")
      return Set.empty
    }

    // Collect distinct values from the query DataFrame
    val values = valuesDf
      .select(column)
      .distinct()
      .collect()
      .map(_.get(0))
      .filter(_ != null)

    if (values.isEmpty) return Set.empty

    // Use the existing method with collected values
    locateFilesWithBloom(column, values, indexDf)
  }
}