package dev.cjfravel.ariadne

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

/** Trait providing index building and maintenance operations for [[Index]] instances.
  *
  * This trait implements the core data pipeline for building and maintaining file-level
  * indexes. It is mixed into [[Index]] via the trait hierarchy and handles the complete
  * update lifecycle:
  *
  * Update pipeline (data flow):
  *  1. [[analyzeFiles]] — Pre-flight scan that computes per-file distinct value
  *     counts for all indexed columns. This information drives batching decisions.
  *  2. [[createOptimalBatches]] — Groups files into batches so that the sum of
  *     `maxDistinctCount` per batch stays below `largeIndexLimit`.
  *     Files that individually exceed the limit are isolated into single-file batches.
  *  3. Per-batch processing (in `updateSingleBatch`):
  *     - Read source files, apply computed indexes, add filename column
  *     - Build regular indexes (array aggregation per file)
  *     - Build exploded field, bloom filter, temporal, and range indexes
  *     - Build auto-bloom filters for columns exceeding `largeIndexLimit`
  *     - [[appendToStaging]] (small columns to staging Delta table)
  *     - [[appendToLargeIndex]] (large columns to per-column Delta tables)
  *  4. Periodic consolidation — Every `stagingConsolidationThreshold`
  *     batches, [[consolidateStaging]] merges staging into the main index via Delta
  *     MERGE (upsert on filename). Auto-compaction may follow via [[maybeAutoCompact]].
  *  5. Final consolidation — After all batches complete, any remaining staged data
  *     is consolidated and the staging table is deleted.
  *
  * Batching strategy:
  * Files are sorted by `maxDistinctCount` (largest first) and packed sequentially into
  * batches. This greedy approach keeps each batch under the large index limit while
  * maximizing batch sizes for efficiency.
  *
  * Staging:
  * Each batch is appended to a transient staging Delta table. Periodic consolidation
  * merges staged rows into the main index, providing fault tolerance (partially
  * processed updates can be recovered).
  *
  * Large index handling:
  * Columns whose array size exceeds `largeIndexLimit` for any file are stored in
  * separate Delta tables under `large_indexes/{column}/` as exploded (one row per
  * value) rather than array-per-file. Auto-bloom filters are built for these columns
  * in the main index to enable pre-filtering at query time.
  *
  * @see [[BloomFilterOperations]] for bloom filter creation and querying
  * @see [[Index.update]] for the public entry point
  */
trait IndexBuildOperations extends BloomFilterOperations {
  self: Index =>

  /** Computes file sizes in bytes for the given files using the Hadoop FileSystem.
    *
    * Files that cannot be found or read are silently skipped with a warning log.
    *
    * @param files set of fully-qualified file paths to measure
    * @return map from file path to size in bytes; missing/unreadable files are omitted
    */
  protected def getFileSizes(files: Set[String]): Map[String, Long] = {
    logger.warn(s"Computing file sizes for ${files.size} files")
    files.toSeq.flatMap { f =>
      try {
        val path = new org.apache.hadoop.fs.Path(f)
        Some(f -> fs.getFileStatus(path).getLen)
      } catch {
        case _: java.io.FileNotFoundException =>
          logger.warn(s"File not found when computing size, skipping: $f")
          None
        case e: java.io.IOException =>
          logger.warn(s"I/O error computing file size for $f: ${e.getMessage}, skipping")
          None
        case e: Exception =>
          logger.warn(s"Unexpected error computing file size for $f: ${e.getMessage}", e)
          None
      }
    }.toMap
  }

  /** Hadoop root path for large index Delta tables (`{storagePath}/large_indexes/`). */
  protected def largeIndexesFilePath: Path =
    new Path(storagePath, "large_indexes")

  /** Hadoop path for the main index Delta table (`{storagePath}/index/`). */
  protected def indexFilePath: Path = new Path(storagePath, "index")

  /** Hadoop path for the staging Delta table (`{storagePath}/staging/`).
    *
    * The staging table accumulates batch results during [[Index.update update]] and
    * is merged into the main index by [[consolidateStaging]].
    */
  protected def stagingFilePath: Path = new Path(storagePath, "staging")

  /** Returns the set of all storage column names across regular, computed,
    * exploded-field, and temporal index types.
    *
    * This is used internally to determine which columns to check for large-index
    * separation and to build aggregation expressions.
    *
    * @return set of column names used for index storage (excludes bloom, range, and auto-bloom)
    */
  protected def storageColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.array_column).toSet ++
      metadata.temporal_indexes.asScala.map(_.column).toSet

  /** Returns the set of range index storage column names (each prefixed with `range_`).
    *
    * @return set of prefixed column names (e.g., `range_event_date`)
    */
  protected def rangeStorageColumns: Set[String] =
    metadata.range_indexes.asScala.map(c => s"range_${c.column}").toSet

  /** Case class to hold file analysis results for batching decisions.
    *
    * @param filename The name of the file
    * @param distinctCounts Map of column name to distinct value count for that file
    * @param maxDistinctCount Maximum distinct count across all indexed columns
    */
  case class FileAnalysis(
    filename: String,
    distinctCounts: Map[String, Long],
    maxDistinctCount: Long
  )

  /** Performs pre-flight analysis on unindexed files to determine optimal batching strategy.
    *
    * Reads the source files, applies computed indexes, and counts distinct values per
    * indexed column per file. The resulting [[FileAnalysis]] objects are used by
    * [[createOptimalBatches]] to group files into batches that stay under the
    * `largeIndexLimit`.
    *
    * If no storage columns are configured (e.g., only bloom or range indexes), returns
    * trivial analyses with zero distinct counts.
    *
    * @note This method calls `.collect()` to bring per-file distinct counts to the
    *       driver. For indexes covering millions of files with many indexed columns,
    *       the collected result set can be large enough to cause driver OOM. Consider
    *       limiting the number of files analyzed per call or increasing driver memory.
    *
    * @param files set of file paths to analyze
    * @return sequence of [[FileAnalysis]] objects with per-column distinct counts;
    *         empty if `files` is empty
    */
  protected def analyzeFiles(files: Set[String]): Seq[FileAnalysis] = {
    if (files.isEmpty) Seq.empty
    else {
      val startTime = System.currentTimeMillis()
      logger.warn(s"Performing pre-flight analysis on ${files.size} files")
      
      val allStorageColumns = storageColumns
      if (allStorageColumns.isEmpty) {
        files.map(f => FileAnalysis(f, Map.empty, 0L)).toSeq
      } else {
    // Read files with just indexed columns + filename
    val baseDf = createBaseDataFrame(files)
    val withComputedIndexes = applyComputedIndexes(baseDf)
    val withFilename = addFilenameColumn(withComputedIndexes, files)
    
    // For each file, count distinct values per indexed column
    val analysisColumns = allStorageColumns.toSeq
    val distinctCountExprs = analysisColumns.map { colName =>
      countDistinct(col(colName)).alias(s"${colName}_distinct")
    }
    
    val fileAnalysisDf = withFilename
      .groupBy("filename")
      .agg(distinctCountExprs.head, distinctCountExprs.tail: _*)
    
    val analysisResults = fileAnalysisDf.collect()
    
    val results = analysisResults.map { row =>
      val filename = row.getAs[String]("filename")
      val distinctCounts = analysisColumns.map { colName =>
        colName -> row.getAs[Long](s"${colName}_distinct")
      }.toMap
      val maxCount = if (distinctCounts.nonEmpty) distinctCounts.values.max else 0L
      
      FileAnalysis(filename, distinctCounts, maxCount)
    }.toSeq

    logger.warn(s"Pre-flight analysis of ${files.size} files completed in ${System.currentTimeMillis() - startTime}ms")
    results
      }
    }
  }

  /** Groups files into batches whose aggregate `maxDistinctCount` stays below
    * `largeIndexLimit`.
    *
    * The algorithm sorts files by `maxDistinctCount` (largest first) and packs them
    * sequentially into batches. When adding a file would push the batch total past
    * the limit, a new batch is started. Files that individually exceed the limit
    * are placed into single-file batches to guarantee progress.
    *
    * @param fileAnalyses sequence of [[FileAnalysis]] objects from [[analyzeFiles]]
    * @return sequence of file batches (each a `Set[String]` of filenames);
    *         empty if `fileAnalyses` is empty
    */
  protected def createOptimalBatches(fileAnalyses: Seq[FileAnalysis]): Seq[Set[String]] = {
    if (fileAnalyses.isEmpty) Seq.empty
    else {
      val allStorageColumns = storageColumns
      if (allStorageColumns.isEmpty) {
        Seq(fileAnalyses.map(_.filename).toSet)
      } else {
        logger.warn(s"Creating optimal batches for ${fileAnalyses.size} files with largeIndexLimit=$largeIndexLimit")
    
    // Separate files that individually exceed the limit (will be processed individually)
    val (largeFiles, regularFiles) = fileAnalyses.partition(_.maxDistinctCount >= largeIndexLimit)
    
    if (largeFiles.nonEmpty) {
      logger.warn(s"Found ${largeFiles.size} files that individually exceed largeIndexLimit and will be processed separately")
    }
    
    val batches = scala.collection.mutable.ListBuffer[Set[String]]()
    
    // Sort files by maxDistinctCount (largest first for better packing)
    val sortedFiles = regularFiles.sortBy(-_.maxDistinctCount)
    
    // Simple sequential batching - group files until sum of maxDistinctCount reaches largeIndexLimit
    var currentBatch = scala.collection.mutable.Set[String]()
    var currentBatchTotal = 0L
    
    for (fileAnalysis <- sortedFiles) {
      val filename = fileAnalysis.filename
      val fileMaxDistinct = fileAnalysis.maxDistinctCount
      
      // Check if adding this file would exceed the limit
      if (currentBatchTotal + fileMaxDistinct >= largeIndexLimit) {
        // Start a new batch if current batch is not empty
        if (currentBatch.nonEmpty) {
          batches += currentBatch.toSet
          currentBatch = scala.collection.mutable.Set[String]()
          currentBatchTotal = 0L
        }
      }
      
      // Add file to current batch
      currentBatch += filename
      currentBatchTotal += fileMaxDistinct
    }
    
    // Add the final batch if it's not empty
    if (currentBatch.nonEmpty) {
      batches += currentBatch.toSet
    }
    
    // Add individual large files as single-file batches
    val largeBatches = largeFiles.map(fa => Set(fa.filename))
    
    val allBatches = batches.toSeq ++ largeBatches
    logger.warn(s"Created ${allBatches.size} batches: ${batches.size} regular batches + ${largeBatches.size} large file batches")
    
    allBatches
      }
    }
  }

  /** Builds regular (array-aggregated) indexes for all configured regular and computed columns.
    *
    * Groups the input data by filename and collects distinct values into arrays via
    * `collect_set`. If no regular or computed indexes are configured, returns a
    * distinct filename-only DataFrame.
    *
    * @param df the base DataFrame with `filename` column and all indexed source columns
    * @return DataFrame with `filename` plus one array column per regular/computed index
    */
  protected def buildRegularIndexes(df: DataFrame): DataFrame = {
    val regularIndexes = metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala
    
    logger.debug(s"Building regular indexes for ${regularIndexes.size} columns: ${regularIndexes.mkString(", ")}")

    if (regularIndexes.nonEmpty) {
      val regularCols = (regularIndexes + "filename").toList
      val selectedDf = df.select(regularCols.map(col): _*).distinct
      val aggExprs = regularIndexes.toList.map(colName => collect_set(col(colName)).alias(colName))
      selectedDf.groupBy("filename").agg(aggExprs.head, aggExprs.tail: _*)
    } else {
      df.select("filename").distinct
    }
  }

  /** Builds exploded field indexes and joins them onto the result DataFrame.
    *
    * For each configured exploded field index, extracts the nested field path from
    * the array column, explodes it, collects distinct values back into an array per
    * file, and joins the result onto `resultDf`.
    *
    * @param baseData the full base DataFrame with all source columns
    * @param resultDf the accumulating result DataFrame to join with (must have `filename`)
    * @return DataFrame with exploded field index columns joined via `full_outer`
    */
  protected def buildExplodedFieldIndexes(baseData: DataFrame, resultDf: DataFrame): DataFrame = {
    val explodedFieldMappings = metadata.exploded_field_indexes.asScala.toSeq
    
    logger.debug(s"Building exploded field indexes for ${explodedFieldMappings.size} mapping(s)")

    explodedFieldMappings.foldLeft(resultDf) { (accumDf, explodedField) =>
      val explodedDf = baseData
        .select("filename", explodedField.array_column)
        .withColumn("temp_exploded", explode(col(s"${explodedField.array_column}.${explodedField.field_path}")))
        .groupBy("filename")
        .agg(collect_set(col("temp_exploded")).alias(explodedField.array_column))

      accumDf.join(explodedDf, Seq("filename"), "full_outer")
    }
  }

  /** Builds temporal indexes storing `Array[Struct(value, max_ts)]` per file.
    *
    * For each temporal index configuration, groups by `(filename, value_column)` to
    * find the maximum timestamp per value per file, then collects the
    * `(value, max_ts)` structs into an array per file. This enables temporal
    * deduplication at query time.
    *
    * @param df the base DataFrame with `filename`, value, and timestamp columns
    * @return DataFrame with `filename` plus one struct-array column per temporal index;
    *         filename-only DataFrame if no temporal indexes are configured
    */
  protected def buildTemporalIndexes(df: DataFrame): DataFrame = {
    val temporalConfigs = metadata.temporal_indexes.asScala.toSeq
    if (temporalConfigs.isEmpty) {
      df.select("filename").distinct()
    } else {
      logger.debug(s"Building temporal indexes for ${temporalConfigs.size} columns: ${temporalConfigs.map(_.column).mkString(", ")}")

      temporalConfigs.foldLeft(df.select("filename").distinct()) { (accumDf, config) =>
        val perFilePerValue = df
          .select("filename", config.column, config.timestamp_column)
          .groupBy("filename", config.column)
          .agg(max(col(config.timestamp_column)).alias("_ariadne_max_ts"))

        val structPerFile = perFilePerValue
          .withColumn("_struct", struct(col(config.column).as("value"), col("_ariadne_max_ts").as("max_ts")))
          .groupBy("filename")
          .agg(collect_set(col("_struct")).alias(config.column))

        accumDf.join(structPerFile, Seq("filename"), "full_outer")
      }
    }
  }

  /** Builds range indexes storing `Struct(min, max)` per file.
    *
    * For each range index configuration, groups by `filename` and computes the
    * `min` and `max` of the column per file. The result is a struct column named
    * `range_{column}`.
    *
    * @param df the base DataFrame with `filename` column and range-indexed source columns
    * @return DataFrame with `filename` plus one range struct column per configured range index;
    *         filename-only DataFrame if no range indexes are configured
    */
  protected def buildRangeIndexes(df: DataFrame): DataFrame = {
    val rangeConfigs = metadata.range_indexes.asScala.toSeq
    if (rangeConfigs.isEmpty) df.select("filename").distinct()
    else {
      logger.warn(s"Building range indexes for ${rangeConfigs.size} columns: ${rangeConfigs.map(_.column).mkString(", ")}")

      rangeConfigs.foldLeft(df.select("filename").distinct()) { (accumDf, config) =>
      val rangeCol = s"range_${config.column}"
      val perFile = df
        .select("filename", config.column)
        .groupBy("filename")
        .agg(
          struct(
            min(col(config.column)).alias("min"),
            max(col(config.column)).alias("max")
          ).alias(rangeCol)
        )
      accumDf.join(perFile, Seq("filename"), "full_outer")
      }
    }
  }

  /** Checks all storage columns for arrays exceeding `largeIndexLimit`
    * and delegates to [[appendToLargeIndex]] for separation.
    *
    * @param df the combined index DataFrame with array columns
    */
  protected def handleLargeIndexes(df: DataFrame): Unit = {
    val allStorageColumns = storageColumns
    if (allStorageColumns.nonEmpty) {
      logger.warn(s"Checking ${allStorageColumns.size} columns for large index separation (limit=$largeIndexLimit)")
      appendToLargeIndex(df)
    }
  }

  /** Appends the processed DataFrame to the staging Delta table.
    *
    * Before writing, any column whose array size meets or exceeds `largeIndexLimit`
    * is nulled out (those values are stored separately via [[appendToLargeIndex]]).
    * The DataFrame is written in append mode with schema merge enabled.
    *
    * @param df the combined index DataFrame to stage
    */
  protected def appendToStaging(df: DataFrame): Unit = {
    val allStorageColumns = storageColumns
    
    // Create small grouped DataFrame (filter out large indexes)
    val smallGroupedDf = if (allStorageColumns.nonEmpty) {
      allStorageColumns.foldLeft(df) {
        case (accumDf, colName) =>
          accumDf.withColumn(
            colName,
            when(size(col(colName)) >= largeIndexLimit, null)
              .otherwise(col(colName))
          )
      }
    } else {
      df
    }

    val rowCount = smallGroupedDf.count()
    logger.warn(s"Appending $rowCount rows to staging at $stagingFilePath")

    smallGroupedDf.write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(stagingFilePath.toString)
  }

  /** Appends large index data to per-column Delta tables under `large_indexes/`.
    *
    * For each storage column, identifies rows where the array size meets or exceeds
    * `largeIndexLimit`, explodes those arrays into individual rows, and writes them
    * to `large_indexes/{column}/`. Before appending, any existing rows for the
    * same filenames are removed via Delta MERGE to prevent duplicates (important
    * during column backfill or re-indexing).
    *
    * @note The `count()` call before the write is intentional: it materializes the
    *       DataFrame to determine whether any large-index rows exist for this column,
    *       avoiding the overhead of a Delta MERGE + write when no rows qualify. This
    *       results in a double computation (count + write), but the alternative—writing
    *       unconditionally—would create empty Delta commits and unnecessary MERGE
    *       operations for columns that are within the limit.
    *
    * @param df the combined index DataFrame with array columns
    */
  protected def appendToLargeIndex(df: DataFrame): Unit = {
    val allStorageColumns = storageColumns
    if (allStorageColumns.nonEmpty) {
    
    val largeGroupedDf = allStorageColumns.foldLeft(df) {
      case (accumDf, colName) =>
        accumDf.withColumn(
          colName,
          when(size(col(colName)) < largeIndexLimit, null)
            .otherwise(col(colName))
        )
    }
    
    // Collect filenames being processed for dedup
    val processedFiles = df.select("filename").distinct()
    
    allStorageColumns.foreach { colName =>
      val columnData = largeGroupedDf
        .select("filename", colName)
        .where(col(colName).isNotNull)
        .withColumn(colName, explode(col(colName)))
        .filter(col(colName).isNotNull)
        .distinct()
      
      val count = columnData.count()
      if (count > 0) {
        val columnPath = new Path(largeIndexesFilePath, colName)
        logger.warn(s"Appending $count rows to large index for column '$colName' at $columnPath")
        
        // Remove existing rows for these files to prevent duplicates during re-indexing
        delta(columnPath) match {
          case Some(deltaTable) =>
            deltaTable.as("target")
              .merge(processedFiles.as("source"), "target.filename = source.filename")
              .whenMatched().delete()
              .execute()
          case None => // No existing table, nothing to dedup
        }
        
        columnData.write
          .format("delta")
          .option("mergeSchema", "true")
          .mode("append")
          .save(columnPath.toString)
      }
    }
    }
  }

  /** Column name prefix for auto-bloom filter storage in the main index table. */
  protected val autoBloomColumnPrefix = "auto_bloom_"

  /** Returns the set of auto-bloom storage column names (each prefixed with `auto_bloom_`).
    *
    * @return set of prefixed column names (e.g., `auto_bloom_user_id`)
    */
  protected def autoBloomStorageColumns: Set[String] =
    metadata.auto_bloom_indexes.asScala.map(c => autoBloomColumnPrefix + c).toSet

  /** Returns the set of columns eligible for auto-bloom filtering.
    *
    * Eligible columns are regular, computed, and exploded-field indexes. Temporal
    * indexes are excluded because they use struct arrays rather than simple value arrays.
    *
    * @return set of eligible column names
    */
  private def autoBloomEligibleColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.array_column).toSet

  /** Tracks the cached DataFrame from [[buildAutoBloomIndexes]] for deferred cleanup.
    *
    * Set during auto-bloom processing and unpersisted after staging is complete.
    */
  @transient protected var lastAutoBloomCache: Option[DataFrame] = None

  /** Builds auto-bloom filters for columns that exceed the large index limit.
    *
    * Scans the combined DataFrame to identify columns with any file whose array size
    * meets or exceeds `largeIndexLimit`. For each such column, a bloom filter is
    * built from the full array and stored in the main index with the `auto_bloom_`
    * prefix. At query time, these bloom filters enable fast pre-filtering to determine
    * which files need to load large index data.
    *
    * The input DataFrame is cached during processing to avoid redundant computation;
    * the cache reference is stored in [[lastAutoBloomCache]] for deferred cleanup.
    * If an exception occurs during processing, the cached DataFrame is unpersisted
    * before the exception is rethrown.
    *
    * @param combinedDf the combined index DataFrame with array columns
    * @return DataFrame with `auto_bloom_{column}` binary columns added for columns
    *         exceeding the limit; unchanged DataFrame if no columns qualify
    */
  protected def buildAutoBloomIndexes(combinedDf: DataFrame): DataFrame = {
    val startTime = System.currentTimeMillis()
    val eligible = autoBloomEligibleColumns
    if (eligible.isEmpty) combinedDf
    else {
      // Cache combinedDf to ensure consistent evaluation when checking column sizes
      // and building bloom filters
      val cachedDf = combinedDf.cache()

      try {
      val columnsExceedingLimit = eligible.filter { colName =>
        cachedDf.columns.contains(colName) &&
        cachedDf
          .select("filename", colName)
          .where(col(colName).isNotNull)
          .where(size(col(colName)) >= largeIndexLimit)
          .count() > 0
      }

      logger.warn(s"Auto-bloom: checked ${eligible.size} columns, ${columnsExceedingLimit.size} exceed limit")

      columnsExceedingLimit.foreach { colName =>
        if (!metadata.auto_bloom_indexes.contains(colName)) {
          metadata.auto_bloom_indexes.add(colName)
        }
      }

      val autoBloomColumns = metadata.auto_bloom_indexes.asScala.toSet
        .intersect(eligible)
        .filter(cachedDf.columns.contains)

      if (autoBloomColumns.isEmpty) {
        cachedDf.unpersist()
        logger.warn(s"Auto-bloom: no columns to build, completed in ${System.currentTimeMillis() - startTime}ms")
        combinedDf
      } else {
        logger.warn(s"Building auto-bloom filters for columns: ${autoBloomColumns.mkString(", ")}")
        val fpr = autoBloomFpr
        val result = autoBloomColumns.foldLeft(cachedDf) { (df, colName) =>
          logger.warn(s"Auto-bloom: building filter for '$colName' with FPR=$fpr")
          val bloomColumn = autoBloomColumnPrefix + colName
          val bloomUdf = createBloomFilterUdf(fpr)
          df.withColumn(bloomColumn, bloomUdf(col(colName)))
        }
        lastAutoBloomCache = Some(cachedDf)
        logger.warn(s"Auto-bloom: build completed in ${System.currentTimeMillis() - startTime}ms")
        result
      }
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to build auto-bloom indexes for $name: ${e.getMessage}", e)
          cachedDf.unpersist()
          throw e
      }
    }
  }

  /** Compacts all Delta tables (main index and large index tables) using Delta OPTIMIZE.
    *
    * Runs Delta Lake's OPTIMIZE command to consolidate small files into larger ones,
    * improving read performance. Processes the main index table first, then each
    * large index column table.
    */
  protected def compactDeltaTables(): Unit = {
    val overallStart = System.currentTimeMillis()
    val largeIdxCols = largeIndexColumns

    delta(indexFilePath).foreach { dt =>
      val compactStart = System.currentTimeMillis()
      logger.warn(s"Compacting main index at $indexFilePath")
      dt.optimize().executeCompaction()
      logger.warn(s"Compacted main index in ${System.currentTimeMillis() - compactStart}ms")
    }

    largeIdxCols.foreach { colName =>
      val columnPath = new Path(largeIndexesFilePath, colName)
      delta(columnPath).foreach { dt =>
        val compactStart = System.currentTimeMillis()
        logger.warn(s"Compacting large index for column $colName at $columnPath")
        dt.optimize().executeCompaction()
        logger.warn(s"Compacted large index '$colName' in ${System.currentTimeMillis() - compactStart}ms")
      }
    }

    logger.warn(s"Compaction completed for main index and ${largeIdxCols.size} large index table(s) in ${System.currentTimeMillis() - overallStart}ms")
  }

  /** Counter tracking batches processed since the last auto-compaction.
    *
    * Incremented after each batch in `updateBatched` and reset to zero after each
    * compaction cycle or at the start of each [[Index.update update]] call to prevent
    * stale counts from a previous `update` from triggering premature compaction.
    */
  protected var batchesSinceCompact: Int = 0

  /** Triggers compaction if the auto-compact threshold has been reached.
    *
    * Checks the [[batchesSinceCompact]] counter against the configured
    * `autoCompactThreshold`. If the threshold is met,
    * compacts all Delta tables and resets the counter to zero.
    * The counter is persisted to metadata so it survives across Spark jobs.
    */
  protected def maybeAutoCompact(): Unit = {
    autoCompactThreshold.foreach { threshold =>
      if (batchesSinceCompact >= threshold) {
        logger.warn(s"Auto-compact threshold reached ($batchesSinceCompact batches), compacting Delta tables")
        compactDeltaTables()
        batchesSinceCompact = 0
        metadata.batches_since_compact = 0
        writeMetadata(metadata)
      }
    }
  }

  /** Vacuums all Delta tables (main index and large index tables) to remove old files.
    *
    * Uses Delta Lake's VACUUM command to delete data files no longer referenced by
    * the transaction log. Temporarily disables the retention duration safety check
    * when `retentionHours` is zero or negative.
    *
    * @param retentionHours number of hours of history to retain (default 168 = 7 days)
    */
  protected def vacuumDeltaTables(retentionHours: Int = 168): Unit = {
    val overallStart = System.currentTimeMillis()
    val largeIdxCols = largeIndexColumns
    val previousCheck = spark.conf.getOption("spark.databricks.delta.retentionDurationCheck.enabled")
    if (retentionHours <= 0) {
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    }
    try {
      delta(indexFilePath).foreach { dt =>
        logger.warn(s"Vacuuming main index at $indexFilePath with retention $retentionHours hours")
        dt.vacuum(retentionHours.toDouble)
      }

      largeIdxCols.foreach { colName =>
        val columnPath = new Path(largeIndexesFilePath, colName)
        delta(columnPath).foreach { dt =>
          logger.warn(s"Vacuuming large index for column $colName at $columnPath with retention $retentionHours hours")
          dt.vacuum(retentionHours.toDouble)
        }
      }

      logger.warn(s"Vacuum completed for main index and ${largeIdxCols.size} large index table(s) in ${System.currentTimeMillis() - overallStart}ms")
    } finally {
      previousCheck match {
        case Some(v) => spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", v)
        case None    => spark.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")
      }
    }
  }

  /** Consolidates staged data into the main index table.
    *
    * Delegates to [[consolidateMainStaging]] which performs a Delta MERGE (upsert)
    * of all staged rows into the main index, then deletes the staging table. Logs
    * the total time taken.
    */
  protected def consolidateStaging(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.warn("Starting consolidation of staged data")
    consolidateMainStaging()
    
    logger.warn(s"Consolidation complete in ${System.currentTimeMillis() - startTime}ms")
  }

  /** Consolidates the main staging table into the main index via Delta MERGE.
    *
    * Performs an upsert (match on `filename`): existing rows are updated, new rows
    * are inserted. Creates the main index if it does not yet exist by writing the
    * staging data directly. Schema auto-merge is temporarily enabled to support
    * new columns added since the index was first created.
    *
    * After successful merge, the staging Delta table is deleted.
    */
  private def consolidateMainStaging(): Unit = {
    if (!exists(stagingFilePath)) {
      logger.warn("No staging data to consolidate for main index")
    } else {
    
    val allStorageColumns = storageColumns ++ bloomStorageColumns ++ rangeStorageColumns ++ autoBloomStorageColumns
    val stagingDf = spark.read.format("delta").load(stagingFilePath.toString)
    val stagingRowCount = stagingDf.count()
    logger.warn(s"Merging $stagingRowCount staged rows into main index")
    
    if (allStorageColumns.nonEmpty) {
      delta(indexFilePath) match {
        case Some(deltaTable) =>
          logger.warn(s"Merging staging data into main index at ${indexFilePath}")
          // Enable schema auto-merge so new index columns evolve the target table
          val previousAutoMerge = spark.conf.getOption("spark.databricks.delta.schema.autoMerge.enabled")
          spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
          try {
            deltaTable
              .as("target")
              .merge(
                stagingDf.as("source"),
                "target.filename = source.filename"
              )
              .whenMatched()
              .updateAll()
              .whenNotMatched()
              .insertAll()
              .execute()
          } finally {
            previousAutoMerge match {
              case Some(v) => spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", v)
              case None => spark.conf.unset("spark.databricks.delta.schema.autoMerge.enabled")
            }
          }
        case None =>
          logger.warn(s"Creating new main index from staging at ${indexFilePath}")
          stagingDf.write
            .format("delta")
            .mode("overwrite")
            .save(indexFilePath.toString)
      }
    } else {
      stagingDf.write
        .format("delta")
        .mode("overwrite")
        .save(indexFilePath.toString)
    }
    
    // Delete staging after successful consolidation
    delete(stagingFilePath)
    logger.warn("Deleted main staging table after consolidation")
    }
  }

}
