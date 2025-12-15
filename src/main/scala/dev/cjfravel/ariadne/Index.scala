package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{Logger, LogManager}
import dev.cjfravel.ariadne.Index.DataFrameOps
import com.google.gson.Gson
import scala.collection.JavaConverters._
import java.util
import java.util.Collections

/** Represents an Index for managing metadata and file-based indexes in Apache
  * Spark.
  *
  * This class provides methods to add, locate, and manage file-based indexing
  * in Spark using Delta Lake. It supports schema enforcement, metadata
  * persistence, and file tracking.
  *
  * @constructor
  *   Private: use the factory methods in the companion object.
  * @param spark
  *   The SparkSession instance.
  * @param name
  *   The name of the index.
  * @param schema
  *   The optional schema of the index.
  */
case class Index private (
    name: String,
    schema: Option[StructType]
)(implicit val spark: SparkSession) extends IndexQueryOperations {

  /** Selected columns for optimized reading. When set, only these columns plus join columns will be read. */
  private var selectedColumns: Option[Seq[String]] = None

  private def fileList: FileList = FileList(IndexPathUtils.fileListName(name))

  /** Path to the storage location of the index. */
  override lazy val storagePath: Path = new Path(IndexPathUtils.storagePath, name)

  /** Selects specific columns for optimized reading.
    *
    *
    * @param columns The column names to select
    * @return This Index instance for method chaining
    * @throws ColumnNotFoundException if any specified column doesn't exist in the schema
    */
  def select(columns: String*): Index = {
    // Validate that all specified columns exist in the schema
    val invalidColumns = columns.filterNot { colName =>
      SchemaHelper.fieldExists(storedSchema, colName)
    }

    if (invalidColumns.nonEmpty) {
      throw new ColumnNotFoundException(s"Columns not found in schema: ${invalidColumns.mkString(", ")}")
    }

    selectedColumns = Some(columns)
    this
  }

  /** Gets the currently selected columns for reading. */
  private[ariadne] def getSelectedColumns: Option[Seq[String]] = selectedColumns

  def hasFile(fileName: String): Boolean = fileList.hasFile(fileName)

  def addFile(fileNames: String*): Unit = fileList.addFile(fileNames: _*)

  /** Helper function to get a list of files that haven't yet been indexed
    *
    * @return
    *   Set of filenames
    */
  private[ariadne] def unindexedFiles: Set[String] = unindexedFiles(spark)
  private[ariadne] def unindexedFiles(spark: SparkSession): Set[String] = {
    val files = fileList.files
    if (files.isEmpty) {
      return Set()
    }
    import spark.implicits._
    index match {
      case Some(df) =>
        files
          .join(df, Seq("filename"), "left_anti")
          .select("filename")
          .as[String]
          .collect()
          .toSet
      case None => files.select("filename").as[String].collect().toSet
    }
  }

  /** Adds an index entry.
    * @param index
    *   The index entry to add.
    * @throws IllegalArgumentException if column is already a bloom index
    */
  def addIndex(index: String): Unit = {
    if (metadata.indexes.contains(index)) return
    
    // Check mutual exclusivity with bloom indexes
    if (metadata.bloom_indexes.asScala.exists(_.column == index)) {
      throw new IllegalArgumentException(
        s"Column '$index' is already a bloom index. " +
        "A column cannot be both a regular index and a bloom index."
      )
    }
    
    metadata.indexes.add(index)
    writeMetadata(metadata)
  }

  /** Adds a bloom filter index for the specified column.
    *
    * Bloom filters are probabilistic data structures that provide:
    * - Guaranteed NO false negatives (if filter says "no", value definitely absent)
    * - Configurable false positive rate (if filter says "yes", value MIGHT be present)
    * - Space-efficient storage (approximately 10 bits per element at 1% FPR)
    *
    * @param column The column name to index with a bloom filter
    * @param fpr False positive rate between 0.0 and 1.0 (default 0.01 = 1%)
    * @throws IllegalArgumentException if column is already a regular or computed index
    * @throws ColumnNotFoundException if column doesn't exist in schema
    */
  def addBloomIndex(column: String, fpr: Double = 0.01): Unit = {
    // Validate FPR range
    require(fpr > 0 && fpr < 1, s"FPR must be between 0 and 1, got: $fpr")
    
    // Check mutual exclusivity with regular indexes
    if (metadata.indexes.contains(column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a regular index. " +
        "A column cannot be both a bloom index and a regular index."
      )
    }
    if (metadata.computed_indexes.containsKey(column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a computed index. " +
        "A column cannot be both a bloom index and a computed index."
      )
    }
    
    // Check if already a bloom index
    if (metadata.bloom_indexes.asScala.exists(_.column == column)) return
    
    // Validate column exists in schema
    if (!SchemaHelper.fieldExists(storedSchema, column)) {
      throw new ColumnNotFoundException(s"Column '$column' not found in schema")
    }
    
    val config = BloomIndexConfig(column, fpr)
    metadata.bloom_indexes.add(config)
    writeMetadata(metadata)
  }

  /** Adds an exploded field index entry.
    * @param arrayColumn
    *   The array column to index.
    * @param fieldPath
    *   The field path to extract from array elements (e.g., "id" or
    *   "profile.user_id").
    * @param asColumn
    *   The column name to use in joins.
    */
  def addExplodedFieldIndex(
      arrayColumn: String,
      fieldPath: String,
      asColumn: String
  ): Unit = {
    // Check if this asColumn is already used
    if (indexes.contains(asColumn)) return

    val explodedFieldMapping =
      ExplodedFieldMapping(arrayColumn, fieldPath, asColumn)
    metadata.exploded_field_indexes.add(explodedFieldMapping)
    writeMetadata(metadata)
  }

  /** Helper function to get all index column names that can be used in joins.
    *
    * @return
    *   Set of all column names that can be used in joins
    */
  def indexes: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.as_column).toSet ++
      metadata.bloom_indexes.asScala.map(_.column).toSet

  def addComputedIndex(name: String, sql_expression: String): Unit = {
    if (metadata.computed_indexes.containsKey(name)) return
    metadata.computed_indexes.put(name, sql_expression)
    writeMetadata(metadata)
  }

  /** Updates the index with new files. */
  def update: Unit = {
    val unindexed = unindexedFiles
    if (unindexed.nonEmpty) {
      logger.warn(s"Updating index for ${unindexed.size} files")
      updateBatched(unindexed)
    }
  }

  /** Updates the index using intelligent batching based on pre-flight analysis.
    *
    * @param files Set of files to process
    */
  private def updateBatched(files: Set[String]): Unit = {
    logger.warn(s"Using intelligent batched update for ${files.size} files")

    // Perform pre-flight analysis to determine optimal batching
    val fileAnalyses = analyzeFiles(files)
    val batches = createOptimalBatches(fileAnalyses)

    logger.warn(s"Processing ${batches.size} batches with consolidation threshold of $stagingConsolidationThreshold")

    var batchesSinceConsolidation = 0

    batches.zipWithIndex.foreach { case (batch, idx) =>
      logger.warn(s"Processing batch ${idx + 1}/${batches.size} with ${batch.size} files")
      updateSingleBatch(batch)
      batchesSinceConsolidation += 1

      // Periodic consolidation for fault tolerance
      if (batchesSinceConsolidation >= stagingConsolidationThreshold) {
        logger.warn(s"Reached consolidation threshold ($stagingConsolidationThreshold batches), consolidating...")
        consolidateStaging()
        batchesSinceConsolidation = 0
      }
    }

    // Always consolidate at the end to finalize all staged data
    if (batchesSinceConsolidation > 0) {
      logger.warn("Consolidating remaining staged data...")
      consolidateStaging()
    }

    logger.warn(s"Completed batched update of ${files.size} files in ${batches.size} batches")
  }

  /** Updates the index with a single batch of files.
    *
    * @param files Set of files to process in this batch
    */
  private def updateSingleBatch(files: Set[String]): Unit = {
    val baseDf = createBaseDataFrame(files)
    val withComputedIndexes = applyComputedIndexes(baseDf)
    val withFilename = withComputedIndexes.withColumn("filename", input_file_name)

    // Build regular indexes
    val regularIndexesDf = buildRegularIndexes(withFilename)
    val withExploded = buildExplodedFieldIndexes(withFilename, regularIndexesDf)
    
    // Build bloom filter indexes
    val bloomDf = buildBloomFilterIndexes(withFilename)
    
    // Combine regular and bloom indexes
    val finalDf = if (bloomIndexConfigs.nonEmpty && withExploded.columns.length > 1) {
      withExploded.join(bloomDf, Seq("filename"), "full_outer")
    } else if (bloomIndexConfigs.nonEmpty) {
      bloomDf
    } else {
      withExploded
    }

    handleLargeIndexes(finalDf)
    appendToStaging(finalDf)
  }
}

/** Companion object for the Index class.
  */
object Index {
  def fileListName(name: String): String = IndexPathUtils.fileListName(name)

  def exists(name: String)(implicit spark: SparkSession): Boolean = IndexPathUtils.exists(name)

  def remove(name: String)(implicit spark: SparkSession): Boolean = IndexPathUtils.remove(name)

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String
  )(implicit spark: SparkSession): Index = apply(name, Some(schema), Some(format), false)

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @param allowSchemaMismatch
    *   The allows schema to be a mismatch.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      allowSchemaMismatch: Boolean
  )(implicit spark: SparkSession): Index = apply(name, Some(schema), Some(format), allowSchemaMismatch)

  /** Factory method to create an Index instance with read options.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @param readOptions
    *   Map of read options for format-specific configuration.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      readOptions: Map[String, String]
  )(implicit spark: SparkSession): Index = apply(name, Some(schema), Some(format), false, Some(readOptions))

  /** Factory method to create an Index instance with read options.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @param allowSchemaMismatch
    *   The allows schema to be a mismatch.
    * @param readOptions
    *   Map of read options for format-specific configuration.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      allowSchemaMismatch: Boolean,
      readOptions: Map[String, String]
  )(implicit spark: SparkSession): Index = apply(
    name,
    Some(schema),
    Some(format),
    allowSchemaMismatch,
    Some(readOptions)
  )

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The optional schema.
    * @param format
    *   The optional format.
    * @param allowSchemaMismatch
    *   The optional flag to allow new schema.
    * @param readOptions
    *   Optional map of read options for format-specific configuration.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: Option[StructType] = None,
      format: Option[String] = None,
      allowSchemaMismatch: Boolean = false,
      readOptions: Option[Map[String, String]] = None
  )(implicit spark: SparkSession): Index = {
    val index = Index(name, schema)(spark)

    val metadataExists = index.metadataExists
    val metadata = if (metadataExists) {
      index.metadata
    } else {
      IndexMetadata(
        null,
        null,
        new util.ArrayList[String](),
        new util.HashMap[String, String](),
        new util.ArrayList[ExplodedFieldMapping](),
        new util.ArrayList[BloomIndexConfig](),
        new util.HashMap[String, String]()
      )
    }

    schema match {
      case Some(s) =>
        if (metadataExists) {
          if (allowSchemaMismatch) {
            if (metadata.schema != s.json) {
              metadata.indexes.forEach(col => {
                if (!SchemaHelper.fieldExists(s, col)) {
                  throw new IndexNotFoundInNewSchemaException(col)
                }
              })
            }
            metadata.schema = s.json
          } else if (metadata.schema != s.json) {
            throw new SchemaMismatchException()
          }
        } else {
          metadata.schema = s.json
        }
      case None =>
        if (!metadataExists) {
          throw new SchemaNotProvidedException()
        }
    }
    format match {
      case Some(f) =>
        if (metadataExists) {
          if (metadata.format != f) {
            throw new FormatMismatchException()
          }
        } else {
          metadata.format = f
        }
      case None =>
        if (!metadataExists) {
          throw new MissingFormatException()
        }
    }

    // Handle read options
    readOptions match {
      case Some(options) =>
        if (metadataExists) {
          // Merge with existing options, with new options taking precedence
          import collection.JavaConverters._
          options.foreach { case (key, value) =>
            metadata.read_options.put(key, value)
          }
        } else {
          // Set initial options
          import collection.JavaConverters._
          options.foreach { case (key, value) =>
            metadata.read_options.put(key, value)
          }
        }
      case None => // Keep existing options
    }

    index.writeMetadata(metadata)
    index
  }

  /** Implicit class for enhancing DataFrame operations with index based
    * operations.
    */
  implicit class DataFrameOps(df: DataFrame) {

    /** Joins the DataFrame with an Index.
      * @param index
      *   The Index instance.
      * @param usingColumns
      *   The columns to use for the join.
      * @param joinType
      *   The type of join (default is "inner").
      * @return
      *   The joined DataFrame.
      */
    def join(
        index: Index,
        usingColumns: Seq[String],
        joinType: String = "inner"
    ): DataFrame = {
      val indexDf = index.joinDf(df, usingColumns)
      df.join(indexDf, usingColumns, joinType)
    }
  }
}
