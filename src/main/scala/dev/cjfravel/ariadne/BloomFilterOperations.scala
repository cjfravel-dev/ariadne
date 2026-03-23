package dev.cjfravel.ariadne

import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/** Trait providing bloom filter operations for [[Index]] instances.
  *
  * Bloom filters are probabilistic data structures used to test set membership.
  * This trait handles the full lifecycle of bloom filter indexes:
  *
  * '''Building''': During index updates, bloom filters are created per file per
  * configured column via [[buildBloomFilterIndexes]]. Values are hashed into a
  * Guava `BloomFilter[CharSequence]` and serialized to `Array[Byte]` for storage
  * in the Delta index table.
  *
  * '''Querying''': At query time, [[locateFilesWithBloom]] and
  * [[locateFilesWithBloomFromDataFrame]] deserialize stored bloom filters and
  * test candidate values, returning the set of files that might contain matches.
  *
  * '''Serialization''': Bloom filters are serialized/deserialized via Guava's
  * `writeTo`/`readFrom` methods using a `StringFunnel` with UTF-8 encoding.
  * All indexed values are converted to strings before hashing.
  *
  * Bloom filters provide:
  *  - Guaranteed no false negatives (if filter says "no", value is definitely absent)
  *  - Configurable false positive rate (if filter says "yes", value might be present)
  *  - Space-efficient storage (approximately 10 bits per element at 1% FPR)
  *
  * UDF mechanics: Bloom filter creation and querying are performed inside Spark UDFs
  * ([[createBloomFilterUdf]] and [[bloomContainsUdf]]) so they can execute on worker
  * nodes. The false positive rate is captured as a local `Double` to ensure
  * serializability across the Spark closure boundary.
  *
  * @see [[BloomIndexConfig]] for per-column configuration
  * @see [[IndexBuildOperations]] for the build pipeline that invokes these operations
  */
trait BloomFilterOperations extends IndexFileOperations {
  self: Index =>

  /** Column name prefix used when storing bloom filter binary data in the index Delta table. */
  protected val bloomColumnPrefix = "bloom_"

  /** Returns the bloom index configurations from the current index metadata.
    *
    * @return sequence of [[BloomIndexConfig]] objects, one per bloom-indexed column
    */
  protected def bloomIndexConfigs: Seq[BloomIndexConfig] =
    metadata.bloom_indexes.asScala.toSeq

  /** Returns the set of source column names that have bloom indexes configured.
    *
    * @return set of column names (without the `bloom_` prefix)
    */
  protected def bloomColumns: Set[String] =
    metadata.bloom_indexes.asScala.map(_.column).toSet

  /** Returns the set of storage column names used for bloom filter binary data.
    *
    * Each name is the source column name prefixed with [[bloomColumnPrefix]].
    *
    * @return set of prefixed column names (e.g., `bloom_user_id`)
    */
  protected def bloomStorageColumns: Set[String] =
    metadata.bloom_indexes.asScala.map(c => bloomColumnPrefix + c.column).toSet

  /** Builds bloom filter indexes for all configured bloom columns.
    *
    * For each [[BloomIndexConfig]], this method:
    *  1. Groups the input data by filename
    *  2. Collects distinct values per file into a set
    *  3. Creates a bloom filter from the collected values using [[createBloomFilterUdf]]
    *  4. Joins the resulting bloom column back onto the accumulating DataFrame
    *
    * If no bloom indexes are configured, returns a distinct filename-only DataFrame.
    *
    * @param df DataFrame containing a `filename` column and all bloom-indexed source columns
    * @return DataFrame with `filename` plus one `bloom_{column}` binary column per configured bloom index
    */
  protected def buildBloomFilterIndexes(df: DataFrame): DataFrame = {
    val configs = bloomIndexConfigs
    if (configs.isEmpty) {
      df.select("filename").distinct()
    } else {
      logger.debug(s"Building bloom filter indexes for ${configs.size} columns: ${configs.map(_.column).mkString(", ")}")

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
  }

  /** Creates a Spark UDF that converts a collected set of values into a serialized bloom filter.
    *
    * The UDF is designed for use inside `agg(bloomUdf(collect_set(...)))`. It:
    *  1. Filters out null values from the input sequence
    *  2. Creates a Guava `BloomFilter` sized for the actual number of non-null elements
    *  3. Inserts each value (converted to `String`) into the filter
    *  4. Serializes the filter to `Array[Byte]` via `ByteArrayOutputStream`
    *
    * The `fprValue` parameter is captured as a local `val` to ensure it is
    * serializable when the UDF closure is shipped to Spark executors.
    *
    * @param fprValue the desired false positive rate (e.g., 0.01 for 1%);
    *                 must be strictly between 0 and 1 (exclusive)
    * @return a UDF that accepts `Seq[Any]` (from `collect_set`) and returns `Array[Byte]`,
    *         or `null` if the input is null or contains only nulls
    * @throws IllegalArgumentException if `fprValue` is not in the range (0, 1)
    */
  protected def createBloomFilterUdf(fprValue: Double): UserDefinedFunction = {
    require(fprValue > 0.0 && fprValue < 1.0,
      s"False positive rate must be between 0 and 1 (exclusive), got: $fprValue")
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
          try {
            bf.writeTo(baos)
            baos.toByteArray
          } finally {
            baos.close()
          }
        }
      }
    }
  }

  /** Deserializes a Guava bloom filter from its byte representation.
    *
    * Uses `BloomFilter.readFrom` with a UTF-8 `StringFunnel`, which must match
    * the funnel used during creation in [[createBloomFilterUdf]].
    *
    * @param bytes serialized bloom filter bytes (produced by `BloomFilter.writeTo`)
    * @return a `BloomFilter[CharSequence]` instance ready for membership queries
    * @throws java.io.IOException if the byte array is malformed or corrupted
    */
  protected def deserializeBloomFilter(
      bytes: Array[Byte]
  ): BloomFilter[CharSequence] = {
    val bais = new ByteArrayInputStream(bytes)
    try {
      BloomFilter.readFrom(bais, Funnels.stringFunnel(StandardCharsets.UTF_8))
    } finally {
      bais.close()
    }
  }

  /** Checks if a value might be contained in a serialized bloom filter.
    *
    * Deserializes the bloom filter and performs a `mightContain` check. Returns
    * `false` if either argument is `null`.
    *
    * @param bloomBytes serialized bloom filter bytes
    * @param value the value to test for membership (converted to `String` internally)
    * @return `true` if the value might be present (probabilistic), `false` if definitely absent
    */
  protected def bloomMightContain(bloomBytes: Array[Byte], value: Any): Boolean = {
    if (bloomBytes == null || value == null) false
    else {
      val bf = deserializeBloomFilter(bloomBytes)
      bf.mightContain(value.toString)
    }
  }

  /** Creates a Spark UDF for checking bloom filter membership at query time.
    *
    * The returned UDF accepts a serialized bloom filter (`Array[Byte]`) and a
    * candidate value (`String`), and returns `true` if the bloom filter indicates
    * the value might be present.
    *
    * @return UDF with signature `(Array[Byte], String) => Boolean`
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

  /** Locates files that might contain any of the given values using bloom filters.
    *
    * Collects all rows from the index, deserializes each file's bloom filter for the
    * specified column, and tests every candidate value. A file is included in the
    * result if any value passes the `mightContain` check.
    *
    * @note This method calls `.collect()` to load all bloom filter binary data to the
    *       driver. For indexes with millions of files, each row carries a serialized
    *       bloom filter (potentially KBs each), which can cause driver OOM. Consider
    *       increasing driver memory for very large indexes.
    *
    * @param column the source column name (without `bloom_` prefix)
    * @param values array of candidate values to search for
    * @param indexDf the index DataFrame containing bloom filter binary columns
    * @return set of filenames whose bloom filters indicate a possible match;
    *         empty set if the bloom column does not exist in the index
    */
  protected def locateFilesWithBloom(
      column: String,
      values: Array[Any],
      indexDf: DataFrame
  ): Set[String] = {
    val bloomColumn = bloomColumnPrefix + column
    logger.warn(s"Querying bloom filter for column '$column' with ${values.length} values")

    if (!indexDf.columns.contains(bloomColumn)) {
      logger.warn(s"Bloom column $bloomColumn not found in index")
      Set.empty
    } else {
      // NOTE: collect() loads all bloom filter byte arrays to the driver. For very large
      // indexes with millions of files, this could cause driver OOM as each row includes
      // a serialized bloom filter (potentially KBs each).
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

      logger.warn(s"Bloom filter for '$column': ${matchingFiles.size} files matched out of total index rows")
      matchingFiles
    }
  }

  /** Locates files that might contain values from a DataFrame using bloom filters.
    *
    * Collects distinct non-null values from the specified column of `valuesDf`,
    * then delegates to [[locateFilesWithBloom]] for the actual bloom filter check.
    *
    * @note This method calls `.collect()` twice: first to gather distinct candidate
    *       values from `valuesDf`, then indirectly via [[locateFilesWithBloom]] to
    *       load all bloom filter binary data to the driver. Both operations can cause
    *       driver OOM for very large DataFrames or indexes with millions of files.
    *
    * @param column the source column name (without `bloom_` prefix)
    * @param valuesDf DataFrame containing the candidate values to search for
    * @param indexDf the index DataFrame containing bloom filter binary columns
    * @return set of filenames whose bloom filters indicate a possible match;
    *         empty set if no non-null values exist or the bloom column is missing
    */
  protected def locateFilesWithBloomFromDataFrame(
      column: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Set[String] = {
    val bloomColumn = bloomColumnPrefix + column

    if (!indexDf.columns.contains(bloomColumn)) {
      logger.warn(s"Bloom column $bloomColumn not found in index")
      Set.empty
    } else {
      // Collect distinct values from the query DataFrame
      val values = valuesDf
        .select(column)
        .distinct()
        .collect()
        .map(_.get(0))
        .filter(_ != null)

      if (values.isEmpty) Set.empty
      else {
        logger.warn(s"Bloom filter query for '$column': ${values.length} distinct values from DataFrame")
        // Use the existing method with collected values
        locateFilesWithBloom(column, values, indexDf)
      }
    }
  }
}
