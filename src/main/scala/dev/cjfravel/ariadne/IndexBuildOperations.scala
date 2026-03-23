package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import io.delta.tables.DeltaTable
import scala.collection.JavaConverters._

/** Trait providing index building and maintenance operations for Index instances.
  *
  * Handles the core data pipeline for building indexes:
  * - Pre-flight file analysis for optimal batching
  * - Regular, exploded, temporal, range, and auto-bloom index construction
  * - Staging and large index management
  * - Delta table compaction and vacuuming
  *
  * Large indexes (columns exceeding `largeIndexLimit` distinct values) are
  * automatically separated into dedicated Delta tables under `large_indexes/`.
  */
trait IndexBuildOperations extends BloomFilterOperations {
  self: Index =>

  /** Computes file sizes in bytes for the given files using HDFS FileSystem.
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
      }
    }.toMap
  }

  /** Hadoop root path for large index delta tables */
  protected def largeIndexesFilePath: Path =
    new Path(storagePath, "large_indexes")

  /** Hadoop path for the index delta table */
  protected def indexFilePath: Path = new Path(storagePath, "index")

  /** Hadoop path for the staging delta table (used during batch processing) */
  protected def stagingFilePath: Path = new Path(storagePath, "staging")

  /** Helper function to get the storage column names for internal use.
    *
    * @return
    *   Set of column names used for internal storage
    */
  protected def storageColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.array_column).toSet ++
      metadata.temporal_indexes.asScala.map(_.column).toSet

  /** Get the set of range index storage column names (with prefix) */
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
    * @param files Set of file names to analyze
    * @return Sequence of FileAnalysis objects with distinct count information
    */
  protected def analyzeFiles(files: Set[String]): Seq[FileAnalysis] = {
    if (files.isEmpty) return Seq.empty
    
    logger.warn(s"Performing pre-flight analysis on ${files.size} files")
    
    val allStorageColumns = storageColumns
    if (allStorageColumns.isEmpty) {
      return files.map(f => FileAnalysis(f, Map.empty, 0L)).toSeq
    }
    
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
    
    analysisResults.map { row =>
      val filename = row.getAs[String]("filename")
      val distinctCounts = analysisColumns.map { colName =>
        colName -> row.getAs[Long](s"${colName}_distinct")
      }.toMap
      val maxCount = if (distinctCounts.nonEmpty) distinctCounts.values.max else 0L
      
      FileAnalysis(filename, distinctCounts, maxCount)
    }.toSeq
  }

  /** Groups files into batches based on their distinct value counts to stay under largeIndexLimit.
    * Uses simplified sequential batching based on maxDistinctCount sum for performance.
    *
    * @param fileAnalyses Sequence of FileAnalysis objects
    * @return Sequence of file batches, where each batch is a set of filenames
    */
  protected def createOptimalBatches(fileAnalyses: Seq[FileAnalysis]): Seq[Set[String]] = {
    if (fileAnalyses.isEmpty) return Seq.empty
    
    val allStorageColumns = storageColumns
    if (allStorageColumns.isEmpty) {
      return Seq(fileAnalyses.map(_.filename).toSet)
    }
    
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

  /** Builds regular indexes DataFrame from the base data.
    *
    * @param df The base DataFrame with filename column
    * @return DataFrame with regular indexes aggregated by filename
    */
  protected def buildRegularIndexes(df: DataFrame): DataFrame = {
    val regularIndexes = metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala
    
    if (regularIndexes.nonEmpty) {
      val regularCols = (regularIndexes + "filename").toList
      val selectedDf = df.select(regularCols.map(col): _*).distinct
      val aggExprs = regularIndexes.toList.map(colName => collect_set(col(colName)).alias(colName))
      selectedDf.groupBy("filename").agg(aggExprs.head, aggExprs.tail: _*)
    } else {
      df.select("filename").distinct
    }
  }

  /** Builds exploded field indexes and joins them to the result DataFrame.
    *
    * @param baseData The base DataFrame with all columns
    * @param resultDf The initial result DataFrame to join with
    * @return DataFrame with exploded field indexes joined
    */
  protected def buildExplodedFieldIndexes(baseData: DataFrame, resultDf: DataFrame): DataFrame = {
    val explodedFieldMappings = metadata.exploded_field_indexes.asScala.toSeq
    
    explodedFieldMappings.foldLeft(resultDf) { (accumDf, explodedField) =>
      val explodedDf = baseData
        .select("filename", explodedField.array_column)
        .withColumn("temp_exploded", explode(col(s"${explodedField.array_column}.${explodedField.field_path}")))
        .groupBy("filename")
        .agg(collect_set(col("temp_exploded")).alias(explodedField.array_column))

      accumDf.join(explodedDf, Seq("filename"), "full_outer")
    }
  }

  /** Builds temporal indexes storing Array[Struct(value, max_ts)] per file.
    *
    * For each temporal index config, groups by (filename, value_column) to find
    * the max timestamp per value per file, then collects into struct arrays.
    *
    * @param df The base DataFrame with filename column and source data
    * @return DataFrame with temporal struct array columns, or filename-only if none configured
    */
  protected def buildTemporalIndexes(df: DataFrame): DataFrame = {
    val temporalConfigs = metadata.temporal_indexes.asScala.toSeq
    if (temporalConfigs.isEmpty) return df.select("filename").distinct()

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

  /** Builds range indexes storing Struct(min, max) per file.
    *
    * For each range index config, groups by filename and computes
    * the min and max of the column per file.
    *
    * @param df The base DataFrame with filename column and source data
    * @return DataFrame with range struct columns, or filename-only if none configured
    */
  protected def buildRangeIndexes(df: DataFrame): DataFrame = {
    val rangeConfigs = metadata.range_indexes.asScala.toSeq
    if (rangeConfigs.isEmpty) return df.select("filename").distinct()

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

  /** Handles large indexes by appending directly to large_indexes/{column}.
    *
    * @param df The DataFrame to process for large indexes
    */
  protected def handleLargeIndexes(df: DataFrame): Unit = {
    val allStorageColumns = storageColumns
    if (allStorageColumns.isEmpty) return

    logger.warn(s"Checking ${allStorageColumns.size} columns for large index separation (limit=$largeIndexLimit)")
    appendToLargeIndex(df)
  }

  /** Appends the processed DataFrame to the staging Delta table.
    *
    * @param df The DataFrame to append to staging
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

    smallGroupedDf.write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(stagingFilePath.toString)
  }

  /** Appends large index data directly to large_indexes/{column} Delta tables.
    * When files already exist in the large index (e.g., during column backfill),
    * existing rows for those files are removed before appending to prevent duplicates.
    *
    * @param df The DataFrame with large index data to append
    */
  protected def appendToLargeIndex(df: DataFrame): Unit = {
    val allStorageColumns = storageColumns
    if (allStorageColumns.isEmpty) return
    
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
      
      if (!columnData.isEmpty) {
        val columnPath = new Path(largeIndexesFilePath, colName)
        logger.warn(s"Appending large index data for column $colName to ${columnPath}")
        
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

  /** Column name prefix for auto-bloom filter storage */
  protected val autoBloomColumnPrefix = "auto_bloom_"

  /** Get the set of auto-bloom storage column names (with prefix) */
  protected def autoBloomStorageColumns: Set[String] =
    metadata.auto_bloom_indexes.asScala.map(c => autoBloomColumnPrefix + c).toSet

  /** Get the set of columns eligible for auto-bloom filtering (excludes temporal columns) */
  private def autoBloomEligibleColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.array_column).toSet

  /** Tracks the cached DataFrame from buildAutoBloomIndexes for deferred cleanup. */
  @transient protected var lastAutoBloomCache: Option[DataFrame] = None

  /** Builds auto-bloom filters for columns that exceed the large index limit.
    *
    * When a column's array exceeds largeIndexLimit for any file, a bloom filter
    * is automatically built and stored in the main index with the auto_bloom_ prefix.
    * At query time, this bloom filter is used to pre-filter which files need to
    * load large index data.
    *
    * @param combinedDf The combined index DataFrame with array columns
    * @return DataFrame with auto-bloom columns added for large columns
    */
  protected def buildAutoBloomIndexes(combinedDf: DataFrame): DataFrame = {
    val eligible = autoBloomEligibleColumns
    if (eligible.isEmpty) return combinedDf

    // Cache combinedDf to ensure consistent evaluation when checking column sizes
    // and building bloom filters
    val cachedDf = combinedDf.cache()

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
      return combinedDf
    }

    logger.warn(s"Building auto-bloom filters for columns: ${autoBloomColumns.mkString(", ")}")
    val fpr = autoBloomFpr
    val result = autoBloomColumns.foldLeft(cachedDf) { (df, colName) =>
      logger.warn(s"Auto-bloom: building filter for '$colName' with FPR=$fpr")
      val bloomColumn = autoBloomColumnPrefix + colName
      val bloomUdf = createBloomFilterUdf(fpr)
      df.withColumn(bloomColumn, bloomUdf(col(colName)))
    }
    lastAutoBloomCache = Some(cachedDf)
    result
  }

  /** Compacts all Delta tables (main index and large index tables) using OPTIMIZE.
    *
    * Runs Delta Lake's OPTIMIZE command on the main index table and all
    * large index column tables to consolidate small files for better read performance.
    */
  protected def compactDeltaTables(): Unit = {
    delta(indexFilePath).foreach { dt =>
      val compactStart = System.currentTimeMillis()
      logger.warn(s"Compacting main index at $indexFilePath")
      dt.optimize().executeCompaction()
      logger.warn(s"Compacted main index in ${System.currentTimeMillis() - compactStart}ms")
    }

    largeIndexColumns.foreach { colName =>
      val columnPath = new Path(largeIndexesFilePath, colName)
      delta(columnPath).foreach { dt =>
        val compactStart = System.currentTimeMillis()
        logger.warn(s"Compacting large index for column $colName at $columnPath")
        dt.optimize().executeCompaction()
        logger.warn(s"Compacted large index '$colName' in ${System.currentTimeMillis() - compactStart}ms")
      }
    }
  }

  /** Counter tracking batches processed since the last auto-compaction.
    * Reset to zero after each compaction cycle.
    */
  protected var batchesSinceCompact: Int = 0

  /** Triggers compaction if the auto-compact threshold has been reached.
    *
    * Checks the `batchesSinceCompact` counter against the configured
    * `autoCompactThreshold`. If the threshold is met, compacts all Delta tables
    * and resets the counter.
    */
  protected def maybeAutoCompact(): Unit = {
    autoCompactThreshold.foreach { threshold =>
      if (batchesSinceCompact >= threshold) {
        logger.warn(s"Auto-compact threshold reached ($batchesSinceCompact batches), compacting Delta tables")
        compactDeltaTables()
        batchesSinceCompact = 0
      }
    }
  }

  /** Vacuums all Delta tables (main index and large index tables) to remove old files.
    *
    * @param retentionHours number of hours of history to retain (default 168 = 7 days)
    */
  protected def vacuumDeltaTables(retentionHours: Int = 168): Unit = {
    val previousCheck = spark.conf.getOption("spark.databricks.delta.retentionDurationCheck.enabled")
    if (retentionHours <= 0) {
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    }
    try {
      delta(indexFilePath).foreach { dt =>
        logger.warn(s"Vacuuming main index at $indexFilePath with retention $retentionHours hours")
        dt.vacuum(retentionHours.toDouble)
      }

      largeIndexColumns.foreach { colName =>
        val columnPath = new Path(largeIndexesFilePath, colName)
        delta(columnPath).foreach { dt =>
          logger.warn(s"Vacuuming large index for column $colName at $columnPath with retention $retentionHours hours")
          dt.vacuum(retentionHours.toDouble)
        }
      }
    } finally {
      previousCheck match {
        case Some(v) => spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", v)
        case None    => spark.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")
      }
    }
  }

  /** Consolidates staged data into the main index table.
    *
    * Merges all data accumulated in the staging Delta table into the main
    * index table, then deletes the staging table.
    */
  protected def consolidateStaging(): Unit = {
    logger.warn("Starting consolidation of staged data")
    consolidateMainStaging()
    
    logger.warn("Consolidation complete")
  }

  /** Consolidates the main staging table into the main index.
    *
    * Performs a Delta MERGE (upsert) of staging rows into the main index,
    * matching on filename. Creates the main index if it does not yet exist.
    */
  private def consolidateMainStaging(): Unit = {
    if (!exists(stagingFilePath)) {
      logger.warn("No staging data to consolidate for main index")
      return
    }
    
    val allStorageColumns = storageColumns ++ bloomStorageColumns ++ rangeStorageColumns ++ autoBloomStorageColumns
    val stagingDf = spark.read.format("delta").load(stagingFilePath.toString)
    
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
