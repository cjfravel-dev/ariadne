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
import java.util.{Collections, UUID}

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

  /** Lock path for file list operations. */
  private def fileListLockPath: Path = new Path(storagePath, ".filelist.lock")

  /** Lock path for index update operations. */
  private def updateLockPath: Path = new Path(storagePath, ".update.lock")

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

  /** Checks if a file is tracked by this index's file list.
    * @param fileName The file path to check
    * @return true if the file is in the file list
    */
  def hasFile(fileName: String): Boolean = fileList.hasFile(fileName)

  /** Adds files to the index's file list for future indexing.
    * Acquires a file list lock to prevent concurrent modifications.
    * @param fileNames One or more file paths to register
    */
  def addFile(fileNames: String*): Unit = {
    val lock = IndexLock(fileListLockPath, name)
    val correlationId = UUID.randomUUID().toString
    lock.acquire(correlationId)
    try {
      fileList.addFile(fileNames: _*)
    } finally {
      lock.release(correlationId)
    }
  }

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

  /** Identifies files already in the index that are missing data for newly added columns.
    *
    * Compares the columns declared in metadata against the columns present in the
    * Delta index table schema. If any metadata columns are missing from the table,
    * all indexed files need to be re-processed for the new columns.
    *
    * @return
    *   Set of filenames needing column backfill
    */
  private[ariadne] def filesNeedingColumnUpdate: Set[String] = {
    import spark.implicits._
    index match {
      case Some(df) =>
        val expectedCols = storageColumns ++ bloomStorageColumns ++
          metadata.temporal_indexes.asScala.map(_.column).toSet ++
          rangeStorageColumns ++ autoBloomStorageColumns
        val existingCols = df.columns.toSet - "filename"
        val missingCols = expectedCols -- existingCols
        if (missingCols.isEmpty) return Set.empty
        logger.warn(s"Detected new index columns not yet in index table: ${missingCols.mkString(", ")}")
        df.select("filename").as[String].collect().toSet
      case None => Set.empty
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

    // Check mutual exclusivity with temporal indexes
    if (metadata.temporal_indexes.asScala.exists(_.column == index)) {
      throw new IllegalArgumentException(
        s"Column '$index' is already a temporal index. " +
        "A column cannot be both a regular index and a temporal index."
      )
    }

    // Check mutual exclusivity with range indexes
    if (metadata.range_indexes.asScala.exists(_.column == index)) {
      throw new IllegalArgumentException(
        s"Column '$index' is already a range index. " +
        "A column cannot be both a regular index and a range index."
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
    if (metadata.temporal_indexes.asScala.exists(_.column == column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a temporal index. " +
        "A column cannot be both a bloom index and a temporal index."
      )
    }
    if (metadata.range_indexes.asScala.exists(_.column == column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a range index. " +
        "A column cannot be both a bloom index and a range index."
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
    // Idempotency check
    if (metadata.exploded_field_indexes.asScala.exists(_.as_column == asColumn)) return

    // Mutual exclusivity checks
    if (metadata.indexes.contains(asColumn)) {
      throw new IllegalArgumentException(
        s"Column '$asColumn' is already a regular index. " +
        "A column cannot be both an exploded field index and a regular index."
      )
    }
    if (metadata.computed_indexes.containsKey(asColumn)) {
      throw new IllegalArgumentException(
        s"Column '$asColumn' is already a computed index. " +
        "A column cannot be both an exploded field index and a computed index."
      )
    }
    if (metadata.bloom_indexes.asScala.exists(_.column == asColumn)) {
      throw new IllegalArgumentException(
        s"Column '$asColumn' is already a bloom index. " +
        "A column cannot be both an exploded field index and a bloom index."
      )
    }
    if (metadata.temporal_indexes.asScala.exists(_.column == asColumn)) {
      throw new IllegalArgumentException(
        s"Column '$asColumn' is already a temporal index. " +
        "A column cannot be both an exploded field index and a temporal index."
      )
    }
    if (metadata.range_indexes.asScala.exists(_.column == asColumn)) {
      throw new IllegalArgumentException(
        s"Column '$asColumn' is already a range index. " +
        "A column cannot be both an exploded field index and a range index."
      )
    }

    val explodedFieldMapping =
      ExplodedFieldMapping(arrayColumn, fieldPath, asColumn)
    metadata.exploded_field_indexes.add(explodedFieldMapping)
    writeMetadata(metadata)
  }

  /** Returns all column names that can be used in joins across all index types.
    *
    * Includes regular, computed, exploded field, bloom, temporal, and range
    * index columns.
    *
    * @return Set of all joinable column names
    */
  def indexes: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.as_column).toSet ++
      metadata.bloom_indexes.asScala.map(_.column).toSet ++
      metadata.temporal_indexes.asScala.map(_.column).toSet ++
      metadata.range_indexes.asScala.map(_.column).toSet

  /** Adds a computed index derived from a SQL expression.
    * @param name The alias name for the computed column
    * @param sql_expression The SQL expression to compute the column value
    * @throws IllegalArgumentException if name conflicts with another index type
    */
  def addComputedIndex(name: String, sql_expression: String): Unit = {
    // Idempotency check
    if (metadata.computed_indexes.containsKey(name)) return

    // Mutual exclusivity checks
    if (metadata.indexes.contains(name)) {
      throw new IllegalArgumentException(
        s"Column '$name' is already a regular index. " +
        "A column cannot be both a computed index and a regular index."
      )
    }
    if (metadata.bloom_indexes.asScala.exists(_.column == name)) {
      throw new IllegalArgumentException(
        s"Column '$name' is already a bloom index. " +
        "A column cannot be both a computed index and a bloom index."
      )
    }
    if (metadata.temporal_indexes.asScala.exists(_.column == name)) {
      throw new IllegalArgumentException(
        s"Column '$name' is already a temporal index. " +
        "A column cannot be both a computed index and a temporal index."
      )
    }
    if (metadata.range_indexes.asScala.exists(_.column == name)) {
      throw new IllegalArgumentException(
        s"Column '$name' is already a range index. " +
        "A column cannot be both a computed index and a range index."
      )
    }
    if (metadata.exploded_field_indexes.asScala.exists(_.as_column == name)) {
      throw new IllegalArgumentException(
        s"Column '$name' is already an exploded field index. " +
        "A column cannot be both a computed index and an exploded field index."
      )
    }

    metadata.computed_indexes.put(name, sql_expression)
    writeMetadata(metadata)
  }

  /** Adds a temporal index for the specified column using a timestamp for versioning.
    *
    * When joining on a temporal index column, only the latest version (by timestamp)
    * of each value is returned. This is useful when multiple files contain the same
    * entity at different points in time.
    *
    * @param column The value column to index on (e.g., "user_id")
    * @param timestampColumn The timestamp column for ordering versions (e.g., "updated_at")
    * @throws IllegalArgumentException if column is already indexed by another type
    * @throws ColumnNotFoundException if either column doesn't exist in schema
    */
  def addTemporalIndex(column: String, timestampColumn: String): Unit = {
    // Idempotency check
    if (metadata.temporal_indexes.asScala.exists(_.column == column)) return

    // Mutual exclusivity checks
    if (metadata.indexes.contains(column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a regular index. " +
        "A column cannot be both a temporal index and a regular index."
      )
    }
    if (metadata.computed_indexes.containsKey(column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a computed index. " +
        "A column cannot be both a temporal index and a computed index."
      )
    }
    if (metadata.bloom_indexes.asScala.exists(_.column == column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a bloom index. " +
        "A column cannot be both a temporal index and a bloom index."
      )
    }
    if (metadata.range_indexes.asScala.exists(_.column == column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a range index. " +
        "A column cannot be both a temporal index and a range index."
      )
    }

    // Validate both columns exist in schema
    if (!SchemaHelper.fieldExists(storedSchema, column)) {
      throw new ColumnNotFoundException(s"Column '$column' not found in schema")
    }
    if (!SchemaHelper.fieldExists(storedSchema, timestampColumn)) {
      throw new ColumnNotFoundException(
        s"Timestamp column '$timestampColumn' not found in schema"
      )
    }

    val config = TemporalIndexConfig(column, timestampColumn)
    metadata.temporal_indexes.add(config)
    writeMetadata(metadata)
  }

  /** Adds a range index for the specified column.
    *
    * Range indexes store min/max values per file, enabling file pruning at
    * query time. Files whose [min, max] range does not overlap with the
    * queried values are skipped.
    *
    * @param column The column to index with min/max range
    * @throws IllegalArgumentException if column is already indexed by another type
    * @throws ColumnNotFoundException if column doesn't exist in schema
    */
  def addRangeIndex(column: String): Unit = {
    // Idempotency check
    if (metadata.range_indexes.asScala.exists(_.column == column)) return

    // Mutual exclusivity checks
    if (metadata.indexes.contains(column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a regular index. " +
        "A column cannot be both a range index and a regular index."
      )
    }
    if (metadata.computed_indexes.containsKey(column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a computed index. " +
        "A column cannot be both a range index and a computed index."
      )
    }
    if (metadata.bloom_indexes.asScala.exists(_.column == column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a bloom index. " +
        "A column cannot be both a range index and a bloom index."
      )
    }
    if (metadata.temporal_indexes.asScala.exists(_.column == column)) {
      throw new IllegalArgumentException(
        s"Column '$column' is already a temporal index. " +
        "A column cannot be both a range index and a temporal index."
      )
    }

    // Validate column exists in schema
    if (!SchemaHelper.fieldExists(storedSchema, column)) {
      throw new ColumnNotFoundException(s"Column '$column' not found in schema")
    }

    val config = RangeIndexConfig(column)
    metadata.range_indexes.add(config)
    writeMetadata(metadata)
  }

  /** Deletes the specified files from the index, large index tables, and file list.
    *
    * Acquires the update lock, removes matching rows from the main index Delta table,
    * all large index Delta tables, and the FileList. If a filename doesn't exist
    * in the index, it is silently ignored.
    *
    * @param filenames
    *   One or more filenames to remove from the index.
    */
  def deleteFiles(filenames: String*): Unit = {
    if (filenames.isEmpty) return

    val startTime = System.currentTimeMillis()
    logger.warn(s"Deleting ${filenames.size} file(s) from index '$name'")
    val lock = IndexLock(updateLockPath, name)
    val correlationId = UUID.randomUUID().toString
    lock.acquire(correlationId)
    try {
      import spark.implicits._
      val toDelete = filenames.toDF("filename")

      // Read file sizes before deleting (to update total)
      val deletedFileSize = delta(indexFilePath).map { dt =>
        val indexDf = dt.toDF
        if (indexDf.columns.contains("file_size")) {
          val result = indexDf
            .join(toDelete, Seq("filename"), "inner")
            .agg(sum("file_size"))
            .head()
          if (result.isNullAt(0)) 0L else result.getLong(0)
        } else 0L
      }.getOrElse(0L)

      // Remove from main index
      delta(indexFilePath).foreach { dt =>
        dt.as("target")
          .merge(toDelete.as("source"), "target.filename = source.filename")
          .whenMatched()
          .delete()
          .execute()
        logger.warn(s"Deleted ${filenames.size} file(s) from main index in ${System.currentTimeMillis() - startTime}ms")
      }

      // Remove from all large index tables
      val largeCols = largeIndexColumns
      largeCols.foreach { colName =>
        val largePath = new Path(largeIndexesFilePath, colName)
        delta(largePath).foreach { dt =>
          dt.as("target")
            .merge(toDelete.as("source"), "target.filename = source.filename")
            .whenMatched()
            .delete()
            .execute()
          logger.warn(s"Deleted file(s) from large index column '$colName'")
        }
      }
      if (largeCols.nonEmpty) {
        logger.warn(s"Deleted from ${largeCols.size} large index tables")
      }

      // Remove from staging table if it exists
      delta(stagingFilePath).foreach { dt =>
        dt.as("target")
          .merge(toDelete.as("source"), "target.filename = source.filename")
          .whenMatched()
          .delete()
          .execute()
        logger.warn(s"Deleted file(s) from staging table")
      }

      // Update total indexed file size
      if (metadata.total_indexed_file_size > 0) {
        metadata.total_indexed_file_size = math.max(0L, metadata.total_indexed_file_size - deletedFileSize)
        writeMetadata(metadata)
      }

      // Remove from file list
      fileList.removeFile(filenames: _*)
      logger.warn(s"Successfully deleted ${filenames.size} file(s) from index '$name' in ${System.currentTimeMillis() - startTime}ms")
    } finally {
      lock.release(correlationId)
    }
  }

  /** Updates the index with new files and backfills newly added columns. */
  def update: Unit = {
    batchesSinceCompact = 0
    val startTime = System.currentTimeMillis()
    val lock = IndexLock(updateLockPath, name)
    val correlationId = UUID.randomUUID().toString
    lock.acquire(correlationId)
    try {
      // Backfill file_size for existing index rows that don't have it
      delta(indexFilePath).foreach { dt =>
        val indexDf = dt.toDF
        if (!indexDf.columns.contains("file_size") || indexDf.where(col("file_size").isNull).limit(1).count() > 0) {
          val nullSizeFiles = if (indexDf.columns.contains("file_size")) {
            indexDf.where(col("file_size").isNull).select("filename").collect().map(_.getString(0)).toSet
          } else {
            indexDf.select("filename").collect().map(_.getString(0)).toSet
          }
          if (nullSizeFiles.nonEmpty) {
            logger.warn(s"Backfilling file sizes for ${nullSizeFiles.size} files")
            val sizes = getFileSizes(nullSizeFiles)
            val sizesBroadcast = spark.sparkContext.broadcast(sizes)
            try {
              val sizeUdf = udf((filename: String) => sizesBroadcast.value.getOrElse(filename, 0L))

              import spark.implicits._
              val updateDf = nullSizeFiles.toSeq.toDF("filename")
                .withColumn("file_size", sizeUdf(col("filename")))

              val previousAutoMerge = spark.conf.getOption("spark.databricks.delta.schema.autoMerge.enabled")
              spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
              try {
                dt.as("target")
                  .merge(updateDf.as("source"), "target.filename = source.filename")
                  .whenMatched()
                  .update(Map("file_size" -> col("source.file_size")))
                  .execute()
              } finally {
                previousAutoMerge match {
                  case Some(v) => spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", v)
                  case None => spark.conf.unset("spark.databricks.delta.schema.autoMerge.enabled")
                }
              }

              // Update total from the full index table
              val totalResult = dt.toDF.agg(sum("file_size")).head()
              metadata.total_indexed_file_size = if (totalResult.isNullAt(0)) 0L else totalResult.getLong(0)
              writeMetadata(metadata)
            } finally {
              sizesBroadcast.destroy()
            }
            logger.warn(s"Backfilled file sizes for ${nullSizeFiles.size} files")
          }
        }
      }

      // Backfill existing files for new columns first, so the index schema
      // is complete before processing new files
      val needsColumnUpdate = filesNeedingColumnUpdate
      if (needsColumnUpdate.nonEmpty) {
        logger.warn(s"Backfilling ${needsColumnUpdate.size} files for new index columns")
        updateBatched(needsColumnUpdate, lock, correlationId, isBackfill = true)
      }
      val unindexed = unindexedFiles
      logger.warn(s"Found ${unindexed.size} unindexed file(s) for index '$name'")
      if (unindexed.nonEmpty) {
        updateBatched(unindexed, lock, correlationId, isBackfill = false)
      }

      // Recalculate total file size if it was unknown (migration from older version)
      if (metadata.total_indexed_file_size < 0) {
        delta(indexFilePath).foreach { dt =>
          val totalSize = if (dt.toDF.columns.contains("file_size")) {
            val result = dt.toDF.agg(sum("file_size")).head()
            if (result.isNullAt(0)) 0L else result.getLong(0)
          } else {
            0L
          }
          metadata.total_indexed_file_size = totalSize
          writeMetadata(metadata)
          logger.warn(f"Recalculated total indexed file size: ${totalSize / (1024.0 * 1024.0 * 1024.0)}%.2f GB")
        }
      }
      logger.warn(s"Update complete for index '$name' in ${System.currentTimeMillis() - startTime}ms")
    } finally {
      lock.release(correlationId)
    }
  }

  /** Compacts all Delta tables belonging to this index using OPTIMIZE.
    * Acquires the update lock to prevent concurrent modifications.
    */
  def compact(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.warn(s"Starting compaction for index '$name'")
    val lock = IndexLock(updateLockPath, name)
    val correlationId = UUID.randomUUID().toString
    lock.acquire(correlationId)
    try {
      compactDeltaTables()
      logger.warn(s"Compaction complete in ${System.currentTimeMillis() - startTime}ms")
    } finally {
      lock.release(correlationId)
    }
  }

  /** Vacuums all Delta tables belonging to this index to remove old files.
    * Acquires the update lock to prevent concurrent modifications.
    *
    * @param retentionHours number of hours of history to retain (default 168 = 7 days)
    */
  def vacuum(retentionHours: Int = 168): Unit = {
    logger.warn(s"Vacuuming index '$name' with retention=$retentionHours hours")
    val lock = IndexLock(updateLockPath, name)
    val correlationId = UUID.randomUUID().toString
    lock.acquire(correlationId)
    try {
      vacuumDeltaTables(retentionHours)
    } finally {
      lock.release(correlationId)
    }
  }

  /** Updates the index using intelligent batching based on pre-flight analysis.
    *
    * @param files Set of files to process
    * @param lock The update lock to refresh during processing
    * @param correlationId The correlation ID for lock refresh
    */
  private def updateBatched(files: Set[String], lock: IndexLock, correlationId: String, isBackfill: Boolean = false): Unit = {
    val updateBatchedStart = System.currentTimeMillis()
    logger.warn(s"Using intelligent batched update for ${files.size} files")

    // Perform pre-flight analysis to determine optimal batching
    val fileAnalyses = analyzeFiles(files)
    val batches = createOptimalBatches(fileAnalyses)

    logger.warn(s"Processing ${batches.size} batches with consolidation threshold of $stagingConsolidationThreshold")

    var batchesSinceConsolidation = 0
    var batchesSinceRefresh = 0

    batches.zipWithIndex.foreach { case (batch, idx) =>
      val batchStart = System.currentTimeMillis()
      logger.warn(s"Processing batch ${idx + 1}/${batches.size} with ${batch.size} files")
      updateSingleBatch(batch, isBackfill)
      logger.warn(s"Batch ${idx + 1}/${batches.size} completed in ${System.currentTimeMillis() - batchStart}ms")
      batchesSinceConsolidation += 1
      batchesSinceRefresh += 1
      batchesSinceCompact += 1

      // Periodic lock refresh to prevent stale lock detection
      if (batchesSinceRefresh >= lockRefreshInterval) {
        lock.refresh(correlationId)
        batchesSinceRefresh = 0
      }

      // Periodic consolidation for fault tolerance
      if (batchesSinceConsolidation >= stagingConsolidationThreshold) {
        logger.warn(s"Reached consolidation threshold ($stagingConsolidationThreshold batches), consolidating...")
        consolidateStaging()
        maybeAutoCompact()
        batchesSinceConsolidation = 0
      }
    }

    // Always consolidate at the end to finalize all staged data
    if (batchesSinceConsolidation > 0) {
      logger.warn("Consolidating remaining staged data...")
      consolidateStaging()
      maybeAutoCompact()
    }

    logger.warn(s"Completed batched update of ${files.size} files in ${batches.size} batches in ${System.currentTimeMillis() - updateBatchedStart}ms")
  }

  /** Updates the index with a single batch of files.
    *
    * @param files Set of files to process in this batch
    */
  private def updateSingleBatch(files: Set[String], isBackfill: Boolean = false): Unit = {
    val singleBatchStart = System.currentTimeMillis()
    logger.warn(s"Processing single batch of ${files.size} files")
    val baseDf = createBaseDataFrame(files)
    val withComputedIndexes = applyComputedIndexes(baseDf)
    val withFilename = addFilenameColumn(withComputedIndexes, files)

    // Compute file sizes from HDFS and add as a column
    val fileSizes = getFileSizes(files)
    val fileSizesBroadcast = spark.sparkContext.broadcast(fileSizes)
    val fileSizeUdf = udf((filename: String) => fileSizesBroadcast.value.getOrElse(filename, 0L))

    // Build regular indexes
    val regularIndexesDf = buildRegularIndexes(withFilename)
    val withExploded = buildExplodedFieldIndexes(withFilename, regularIndexesDf)
    
    // Build bloom filter indexes
    val bloomDf = buildBloomFilterIndexes(withFilename)

    // Build temporal indexes (struct arrays with value + max_ts)
    val temporalDf = buildTemporalIndexes(withFilename)
    
    // Combine all index types
    var combinedDf = withExploded

    if (bloomIndexConfigs.nonEmpty && combinedDf.columns.length > 1) {
      combinedDf = combinedDf.join(bloomDf, Seq("filename"), "full_outer")
    } else if (bloomIndexConfigs.nonEmpty) {
      combinedDf = bloomDf
    }

    val temporalConfigs = metadata.temporal_indexes.asScala.toSeq
    if (temporalConfigs.nonEmpty && combinedDf.columns.length > 1) {
      combinedDf = combinedDf.join(temporalDf, Seq("filename"), "full_outer")
    } else if (temporalConfigs.nonEmpty) {
      combinedDf = temporalDf
    }

    // Build range indexes (struct with min/max per file)
    val rangeDf = buildRangeIndexes(withFilename)

    val rangeConfigs = metadata.range_indexes.asScala.toSeq
    if (rangeConfigs.nonEmpty && combinedDf.columns.length > 1) {
      combinedDf = combinedDf.join(rangeDf, Seq("filename"), "full_outer")
    } else if (rangeConfigs.nonEmpty) {
      combinedDf = rangeDf
    }

    // Build auto-bloom filters for columns that exceed largeIndexLimit
    combinedDf = buildAutoBloomIndexes(combinedDf)

    combinedDf = combinedDf.withColumn("file_size", fileSizeUdf(col("filename")))

    handleLargeIndexes(combinedDf)
    appendToStaging(combinedDf)

    // Clean up cached DataFrame from auto-bloom processing
    lastAutoBloomCache.foreach(_.unpersist())
    lastAutoBloomCache = None

    // Update total indexed file size (skip for backfill — already counted)
    if (!isBackfill) {
      val batchFileSize = fileSizes.values.sum
      if (metadata.total_indexed_file_size < 0) {
        metadata.total_indexed_file_size = batchFileSize
      } else {
        metadata.total_indexed_file_size = metadata.total_indexed_file_size + batchFileSize
      }
    }

    fileSizesBroadcast.destroy()

    // Persist any metadata changes (e.g., auto-bloom column detection) after data is safely staged
    writeMetadata(metadata)
    logger.warn(s"Single batch of ${files.size} files completed in ${System.currentTimeMillis() - singleBatchStart}ms")
  }
}

/** Companion object for the Index class.
  */
object Index {
  /** Returns the file list name for an index.
    * @param name The index name
    * @return The file list identifier
    */
  def fileListName(name: String): String = IndexPathUtils.fileListName(name)

  /** Checks if an index exists.
    * @param name The index name
    * @return true if the index exists
    */
  def exists(name: String)(implicit spark: SparkSession): Boolean = IndexPathUtils.exists(name)

  /** Removes an index and its associated data.
    * @param name The index name
    * @return true if removal was successful
    * @throws IndexNotFoundException if the index does not exist
    */
  def remove(name: String)(implicit spark: SparkSession): Boolean = IndexPathUtils.remove(name)

  /** Convenience factory: creates or reconnects with schema, format, and no schema mismatch.
    *
    * @param name   Unique index name
    * @param schema Spark schema of the data files
    * @param format Data file format (e.g., "parquet")
    * @return A fully initialized Index instance
    * @see [[apply(name:String,schema:Option[StructType],format:Option[String],allowSchemaMismatch:Boolean,readOptions:Option[Map[String,String]])*]]
    */
  def apply(
      name: String,
      schema: StructType,
      format: String
  )(implicit spark: SparkSession): Index = apply(name, Some(schema), Some(format), false)

  /** Convenience factory: creates or reconnects with optional schema mismatch tolerance.
    *
    * @param name               Unique index name
    * @param schema             Spark schema of the data files
    * @param format             Data file format (e.g., "parquet")
    * @param allowSchemaMismatch When true, allows updating the stored schema
    * @return A fully initialized Index instance
    * @see [[apply(name:String,schema:Option[StructType],format:Option[String],allowSchemaMismatch:Boolean,readOptions:Option[Map[String,String]])*]]
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      allowSchemaMismatch: Boolean
  )(implicit spark: SparkSession): Index = apply(name, Some(schema), Some(format), allowSchemaMismatch)

  /** Convenience factory: creates or reconnects with read options.
    *
    * @param name        Unique index name
    * @param schema      Spark schema of the data files
    * @param format      Data file format (e.g., "parquet")
    * @param readOptions Format-specific read options (e.g., CSV delimiter)
    * @return A fully initialized Index instance
    * @see [[apply(name:String,schema:Option[StructType],format:Option[String],allowSchemaMismatch:Boolean,readOptions:Option[Map[String,String]])*]]
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      readOptions: Map[String, String]
  )(implicit spark: SparkSession): Index = apply(name, Some(schema), Some(format), false, Some(readOptions))

  /** Convenience factory: creates or reconnects with schema mismatch tolerance and read options.
    *
    * @param name               Unique index name
    * @param schema             Spark schema of the data files
    * @param format             Data file format (e.g., "parquet")
    * @param allowSchemaMismatch When true, allows updating the stored schema
    * @param readOptions        Format-specific read options (e.g., CSV delimiter)
    * @return A fully initialized Index instance
    * @see [[apply(name:String,schema:Option[StructType],format:Option[String],allowSchemaMismatch:Boolean,readOptions:Option[Map[String,String]])*]]
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
        new util.ArrayList[TemporalIndexConfig](),
        new util.HashMap[String, String](),
        new util.ArrayList[RangeIndexConfig](),
        new util.ArrayList[String](),
        -1L
      )
    }

    schema match {
      case Some(s) =>
        if (metadataExists) {
          if (allowSchemaMismatch) {
            if (metadata.schema != s.json) {
              // Validate regular indexes
              metadata.indexes.forEach(col => {
                if (!SchemaHelper.fieldExists(s, col)) {
                  throw new IndexNotFoundInNewSchemaException(col)
                }
              })
              // Validate bloom indexes
              metadata.bloom_indexes.asScala.foreach { bi =>
                if (!SchemaHelper.fieldExists(s, bi.column)) {
                  throw new IndexNotFoundInNewSchemaException(bi.column)
                }
              }
              // Validate temporal indexes
              metadata.temporal_indexes.asScala.foreach { ti =>
                if (!SchemaHelper.fieldExists(s, ti.column)) {
                  throw new IndexNotFoundInNewSchemaException(ti.column)
                }
                if (!SchemaHelper.fieldExists(s, ti.timestamp_column)) {
                  throw new IndexNotFoundInNewSchemaException(ti.timestamp_column)
                }
              }
              // Validate range indexes
              metadata.range_indexes.asScala.foreach { ri =>
                if (!SchemaHelper.fieldExists(s, ri.column)) {
                  throw new IndexNotFoundInNewSchemaException(ri.column)
                }
              }
              // Validate computed indexes
              metadata.computed_indexes.keySet().asScala.foreach { ci =>
                // Computed indexes use SQL expressions, not schema fields directly
                // We validate the output column name exists conceptually but
                // cannot validate the expression references without executing it
              }
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
          options.foreach { case (key, value) =>
            metadata.read_options.put(key, value)
          }
        } else {
          // Set initial options
          options.foreach { case (key, value) =>
            metadata.read_options.put(key, value)
          }
        }
      case None => // Keep existing options
    }

    index.writeMetadata(metadata)
    index
  }

  /** Implicit enrichment enabling `df.join(index, columns, joinType)` syntax.
    *
    * This provides the reverse join direction compared to [[Index.join]]:
    * the driving DataFrame is on the left and the index-located data is on the right.
    *
    * @param df The DataFrame to enrich with implicit join capability
    */
  implicit class DataFrameOps(df: DataFrame) {

    /** Joins this DataFrame with the data files identified by the Index.
      *
      * Locates relevant data files via the index, reads them, applies temporal
      * deduplication if configured, and joins the result with this DataFrame.
      *
      * @param index       The Index instance to join against
      * @param usingColumns Column names to join on (must be indexed columns)
      * @param joinType    Spark join type: "inner", "left_outer", etc. (default "inner")
      * @return The joined DataFrame
      * @throws ColumnNotFoundException if join columns are not in the schema or indexes
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
