package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Row}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

/** Trait providing query and file location operations for Index instances.
  *
  * This is the top-level trait in the Index trait hierarchy, providing:
  * - File location via regular, bloom, temporal, range, and auto-bloom indexes
  * - Multi-column intersection with AND semantics across index types
  * - Index statistics and diagnostics
  * - Delta table management (repartitioning, large index loading)
  *
  * File location uses a staged collection strategy: distinct filenames are
  * written to temporary CSV storage before collection to avoid executor
  * memory pressure on large result sets.
  */
trait IndexQueryOperations extends IndexJoinOperations {
  self: Index =>

  /** Conditionally repartitions a DataFrame if indexRepartitionCount is
    * configured. This helps avoid FetchFailedExceptions when working with very
    * large index DataFrames by spreading data across more partitions before
    * expensive operations like explode.
    *
    * @param df
    *   The DataFrame to potentially repartition
    * @return
    *   Repartitioned DataFrame if configured, otherwise the original DataFrame
    */
  protected def maybeRepartition(df: DataFrame): DataFrame = {
    indexRepartitionCount match {
      case Some(count) =>
        logger.warn(s"Repartitioning DataFrame to $count partitions")
        df.repartition(count)
      case None => df
    }
  }

  /** Helper function to load the index
    *
    * @return
    *   DataFrame containing latest version of the index
    */
  protected def index: Option[DataFrame] = {
    delta(indexFilePath).map(_.toDF)
  }

  /** Returns the set of column names that have large index Delta tables.
    *
    * @return
    *   Set of column names with large index storage
    */
  protected def largeIndexColumns: Set[String] = {
    if (!exists(largeIndexesFilePath)) Set.empty
    else {
      fs.listStatus(largeIndexesFilePath)
        .filter(_.isDirectory)
        .map(_.getPath)
        .filter(path =>
          exists(path) && DeltaTable.isDeltaTable(spark, path.toString)
        )
        .map(_.getName)
        .toSet
    }
  }

  /** Loads a large index Delta table for a specific column. Large indexes
    * store data in exploded (filename, value) row form rather than as arrays.
    *
    * @param colName
    *   The column name
    * @return
    *   DataFrame with (filename, colName) rows, or None if no large index
    *   exists
    */
  protected def loadLargeIndex(colName: String): Option[DataFrame] = {
    val columnPath = new Path(largeIndexesFilePath, colName)
    try {
      if (
        exists(columnPath) && DeltaTable
          .isDeltaTable(spark, columnPath.toString)
      )
        Some(spark.read.format("delta").load(columnPath.toString))
      else None
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to load large index for column '$colName' in index '$name': ${e.getMessage}")
        None
    }
  }

  /** Returns a unified (filename, colName) DataFrame for a regular index
    * column by exploding the main index arrays and unioning with any large
    * index rows that exist for that column.
    *
    * @param indexDf
    *   The main index DataFrame (may be repartitioned)
    * @param colName
    *   The storage column name
    * @param bloomCandidateFiles
    *   Optional set of files that passed auto-bloom pre-filtering.
    *   When provided, only large index rows for these files are included.
    * @return
    *   DataFrame with (filename, colName) scalar rows
    */
  protected def loadColumnIndex(
      indexDf: DataFrame,
      colName: String,
      bloomCandidateFiles: Option[Set[String]] = None
  ): DataFrame = {
    val mainRows = indexDf
      .select(col("filename"), explode(col(colName)).alias(colName))
    loadLargeIndex(colName) match {
      case Some(largeDf) =>
        bloomCandidateFiles match {
          case Some(candidates) if candidates.nonEmpty =>
            logger.warn(s"Filtering large index for $colName to ${candidates.size} bloom-candidate files")
            val filteredLarge = largeDf.where(col("filename").isin(candidates.toSeq: _*))
            mainRows.union(filteredLarge)
          case _ =>
            mainRows.union(largeDf)
        }
      case None => mainRows
    }
  }

  /** Returns a unified (filename, _value, _max_ts) DataFrame for a temporal
    * index column by exploding the main index struct arrays and unioning with
    * any large index rows that exist for that column.
    *
    * @param indexDf
    *   The main index DataFrame (may be repartitioned)
    * @param colName
    *   The temporal column name
    * @return
    *   DataFrame with (filename, _value, _max_ts) rows
    */
  protected def loadTemporalColumnIndex(
      indexDf: DataFrame,
      colName: String
  ): DataFrame = {
    val mainRows = indexDf
      .select(col("filename"), explode(col(colName)).alias("_temporal"))
      .select(
        col("filename"),
        col("_temporal.value").alias("_value"),
        col("_temporal.max_ts").alias("_max_ts")
      )
    loadLargeIndex(colName) match {
      case Some(largeDf) =>
        val largeRows = largeDf
          .select(
            col("filename"),
            col(s"$colName.value").alias("_value"),
            col(s"$colName.max_ts").alias("_max_ts")
          )
        mainRows.union(largeRows)
      case None => mainRows
    }
  }

  /** Locates files matching the given index values across all index types.
    *
    * Queries regular, bloom, temporal, range, and auto-bloom indexes in
    * parallel and intersects results with AND semantics. Each index type
    * independently returns candidate files, and only files present in all
    * queried categories are included in the final result.
    *
    * @param indexes
    *   A map of index column names to arrays of values to search for.
    *   Columns are automatically routed to the appropriate index type
    *   (bloom, temporal, range, or regular).
    * @return
    *   A set of file paths matching all query criteria, or an empty set
    *   if no matches are found or no index exists
    *
    * @note If a staging table exists, its contents are collected to the driver
    *       for merging. This may cause driver OOM for very large staging tables.
    *
    * @example
    * {{{
    * val matchingFiles = index.locateFiles(Map("userId" -> Set("u1", "u2")))
    * }}}
    */
  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
    require(indexes != null, "columnValues must not be null")
    val locateStart = System.currentTimeMillis()
    logger.warn(s"locateFiles: querying columns ${indexes.keys.mkString(", ")} with ${indexes.values.map(_.length).sum} total values")
    index match {
      case Some(df) =>
        // Separate bloom, temporal, range, and regular index queries
        val bloomColumnSet = bloomColumns
        val temporalColumnSet = metadata.temporal_indexes.asScala.map(_.column).toSet
        val rangeColumnSet = metadata.range_indexes.asScala.map(_.column).toSet
        val (bloomQueries, nonBloomQueries) = indexes.partition {
          case (col, _) => bloomColumnSet.contains(col)
        }
        val (temporalQueries, nonTemporalQueries) = nonBloomQueries.partition {
          case (col, _) => temporalColumnSet.contains(col)
        }
        val (rangeQueries, regularQueries) = nonTemporalQueries.partition {
          case (col, _) => rangeColumnSet.contains(col)
        }

        // Get files from bloom filters
        val bloomStart = System.currentTimeMillis()
        val bloomFiles = if (bloomQueries.nonEmpty) {
          bloomQueries.map { case (col, values) =>
            locateFilesWithBloom(col, values, df)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }
        val bloomMs = System.currentTimeMillis() - bloomStart

        // Get files from temporal indexes (pruned to latest timestamp per value)
        val temporalStart = System.currentTimeMillis()
        val temporalFiles = if (temporalQueries.nonEmpty) {
          temporalQueries.map { case (column, values) =>
            locateFilesWithTemporal(column, values, df)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }
        val temporalMs = System.currentTimeMillis() - temporalStart

        // Get files from range indexes
        val rangeStart = System.currentTimeMillis()
        val rangeFiles = if (rangeQueries.nonEmpty) {
          rangeQueries.map { case (column, values) =>
            locateFilesWithRange(column, values, df)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }
        val rangeMs = System.currentTimeMillis() - rangeStart

        // Get files from regular indexes
        val regularStart = System.currentTimeMillis()
        val regularFiles = if (regularQueries.nonEmpty) {
          locateFilesRegular(regularQueries, df)
        } else {
          Set.empty[String]
        }
        val regularMs = System.currentTimeMillis() - regularStart

        // Combine results - intersect across types that returned results (AND semantics)
        // Track which categories were queried (had input), not just which returned results
        val queriedResults: Seq[Set[String]] = Seq(
          if (bloomQueries.nonEmpty) Some(bloomFiles) else None,
          if (temporalQueries.nonEmpty) Some(temporalFiles) else None,
          if (rangeQueries.nonEmpty) Some(rangeFiles) else None,
          if (regularQueries.nonEmpty) Some(regularFiles) else None
        ).flatten

        val allFiles = if (queriedResults.isEmpty) {
          Set.empty[String]
        } else {
          queriedResults.reduce(_ intersect _)
        }
        if (debugEnabled) {
          logger.warn(
            s"[debug] Cross-type combination: bloom=${bloomFiles.size}, temporal=${temporalFiles.size}, " +
              s"range=${rangeFiles.size}, regular=${regularFiles.size} -> final=${allFiles.size}"
          )
        }
        val totalMs = System.currentTimeMillis() - locateStart
        logger.warn(
          s"locateFiles: ${allFiles.size} files matched in ${totalMs}ms " +
            s"(bloom=${bloomMs}ms, temporal=${temporalMs}ms, range=${rangeMs}ms, regular=${regularMs}ms)"
        )
        if (allFiles.isEmpty) Set.empty else allFiles
      case None =>
        logger.warn(s"Index table not found for index '$name'; returning empty file set")
        Set.empty
    }
  }

  /** Collects filenames via staging through temporary CSV storage.
    *
    * Separates the distributed distinct operation from the driver-side collect
    * to avoid executor memory pressure on large result sets. Writes distinct
    * filenames to a temporary CSV path, then reads them back and collects.
    * The temporary path is cleaned up in a finally block.
    *
    * @note All distinct filenames are collected to the driver. For indexes with
    *   millions of files, this can cause driver OOM.
    *
    * @param resultDF
    *   DataFrame containing a "filename" column to collect
    * @return
    *   Set of distinct filenames collected from the staged CSV
    */
  private def collectFilenamesViaStaging(resultDF: DataFrame): Set[String] = {
    val stagingStart = System.currentTimeMillis()
    val tempPath = new Path(
      IndexPathUtils.tempPath,
      s"query_files_${System.currentTimeMillis()}_${java.util.UUID.randomUUID()}"
    )

    try {
      if (debugEnabled) {
        logger.warn(s"[debug] collectFilenamesViaStaging: writing distinct filenames to $tempPath")
      }
      // Write distinct filenames to temp location (distributed operation)
      // Using CSV for simplicity and easier debugging (single column of strings)
      resultDF
        .distinct()
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(tempPath.toString)

      if (debugEnabled) {
        logger.warn(s"[debug] collectFilenamesViaStaging: CSV write completed in ${System.currentTimeMillis() - stagingStart}ms")
      }

      logger.debug(s"Staged filenames to $tempPath")

      // Read back from temp (simple, optimized operation)
      val stagedFiles = spark.read
        .option("header", "true")
        .csv(tempPath.toString)
        .select("filename")

      val collectedFiles = stagedFiles.collect()
      val fileCount = collectedFiles.length
      logger.warn(s"Collecting $fileCount distinct filenames from staging")

      val files = collectedFiles
        .map(_.getString(0))
        .filter(_ != null)
        .toSet

      if (debugEnabled) {
        logger.warn(s"[debug] collectFilenamesViaStaging: complete in ${System.currentTimeMillis() - stagingStart}ms, $fileCount files collected")
      }

      files
    } finally {
      // Cleanup temp location
      try {
        if (fs.exists(tempPath)) {
          fs.delete(tempPath, true)
          logger.debug(s"Cleaned up temp path $tempPath")
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to cleanup temp path $tempPath: ${e.getMessage}")
      }
    }
  }

  /** Gets candidate files from the auto-bloom filter for a specific column.
    *
    * Collects all bloom filter byte arrays from the index to the driver and
    * tests each value against them. Files whose bloom filter is null are
    * included as candidates for backward compatibility with indexes built
    * before auto-bloom was added.
    *
    * @note This method collects all (filename, bloom_filter_bytes) rows to the
    *   driver. For very large indexes (100k+ files), this can cause driver OOM.
    *   Monitor driver memory when using auto-bloom on large indexes.
    *
    * @param column
    *   The storage column name to check for auto-bloom filtering
    * @param values
    *   Array of values to probe against each file's bloom filter
    * @param indexDf
    *   The main index DataFrame containing auto-bloom binary columns
    * @return
    *   Some(set of candidate filenames) if an auto-bloom index exists for the
    *   column, None otherwise
    */
  protected def getAutoBloomCandidates(
      column: String,
      values: Array[Any],
      indexDf: DataFrame
  ): Option[Set[String]] = {
    if (!metadata.auto_bloom_indexes.asScala.contains(column)) None
    else {
      val autoBloomCol = s"auto_bloom_${column}"
      if (!indexDf.columns.contains(autoBloomCol)) None
      else {
        val rowCount = indexDf.count()
        if (rowCount > 100000) {
          logger.warn(s"getAutoBloomCandidates: collecting $rowCount rows to driver for column '$column' — risk of driver OOM on very large indexes")
        }

        val allRows = indexDf
          .select("filename", autoBloomCol)
          .collect()

        val candidates = allRows
          .filter { row =>
            val bloomBytes = row.getAs[Array[Byte]](autoBloomCol)
            if (bloomBytes == null) true
            else values.exists(v => bloomMightContain(bloomBytes, v))
          }
          .map(_.getString(0))
          .toSet

        val totalFiles = allRows.length
        val reduction = if (totalFiles > 0) 100 - (candidates.size * 100 / totalFiles) else 0
        logger.warn(s"Auto-bloom filter for '$column': $totalFiles total files -> ${candidates.size} candidates ($reduction% reduction)")
        Some(candidates)
      }
    }
  }

  /** Gets candidate files from the auto-bloom filter using values extracted
    * from a DataFrame.
    *
    * Collects distinct non-null values from the given DataFrame column, then
    * delegates to [[getAutoBloomCandidates]] for the actual bloom filter probing.
    *
    * @note This method collects distinct values from `valuesDf` to the driver
    *   and then delegates to [[getAutoBloomCandidates]], which collects all
    *   bloom filter rows. Both collections can cause driver OOM on large indexes.
    *
    * @param storageColumn
    *   The storage column name in the index (used to look up the auto-bloom)
    * @param joinColumn
    *   The column name in `valuesDf` containing the query values
    * @param valuesDf
    *   DataFrame containing the values to probe against bloom filters
    * @param indexDf
    *   The main index DataFrame containing auto-bloom binary columns
    * @return
    *   Some(set of candidate filenames) if an auto-bloom index exists,
    *   None otherwise
    */
  protected def getAutoBloomCandidatesFromDf(
      storageColumn: String,
      joinColumn: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Option[Set[String]] = {
    if (!metadata.auto_bloom_indexes.asScala.contains(storageColumn)) None
    else {
      val autoBloomCol = s"auto_bloom_${storageColumn}"
      if (!indexDf.columns.contains(autoBloomCol)) None
      else {
        val values = valuesDf
          .select(joinColumn)
          .distinct()
          .collect()
          .map(_.get(0))
          .filter(_ != null)

        if (values.isEmpty) None
        else getAutoBloomCandidates(storageColumn, values, indexDf)
      }
    }
  }

  /** Locates files using regular (non-bloom, non-temporal, non-range) indexes.
    *
    * For each column, explodes the index arrays and filters to matching values,
    * optionally using auto-bloom pre-filtering. When multiple columns are queried,
    * results are intersected (AND semantics) via inner joins on filename.
    *
    * @param indexes
    *   A map of storage column names to arrays of values to match
    * @param df
    *   The main index DataFrame
    * @return
    *   Set of filenames matching all specified column values
    */
  private def locateFilesRegular(
      indexes: Map[String, Array[Any]],
      df: DataFrame
  ): Set[String] = {
    if (indexes.isEmpty) {
      Set.empty
    } else {
      // Filter null values from each column — isin() throws on an empty array
      val filteredIndexes = indexes.map { case (column, values) =>
        column -> values.filter(_ != null)
      }
      if (filteredIndexes.exists(_._2.isEmpty)) {
        val emptyColumns = filteredIndexes.filter(_._2.isEmpty).keys.mkString(", ")
        logger.debug(s"locateFilesRegular: columns [$emptyColumns] have no non-null values, returning empty result")
        Set.empty[String]
      } else {
        val perColumnFiles = filteredIndexes.map { case (column, values) =>
          val bloomCandidates = getAutoBloomCandidates(column, values, df)
          loadColumnIndex(df, column, bloomCandidates)
            .where(col(column).isin(values: _*))
            .select("filename")
            .distinct
        }.toSeq

        val intersectedDF = if (perColumnFiles.size == 1) {
          perColumnFiles.head
        } else {
          perColumnFiles.reduce((a, b) => a.join(b, Seq("filename"), "inner"))
        }

        val result = collectFilenamesViaStaging(intersectedDF)
        if (debugEnabled && indexes.size > 1) {
          logger.warn(s"[debug] Multi-column intersection: ${indexes.size} columns, result: ${result.size} files")
        }
        result
      }
    }
  }

  /** Locates files using temporal indexes, pruning to only files containing
    * the latest version of each value (by max timestamp).
    *
    * Uses a window function partitioned by value, ordered by max_ts descending,
    * keeping only rank 1 to ensure only the most recent file per value is returned.
    *
    * @param column The temporal index value column name
    * @param values Array of values to search for in the temporal index
    * @param df The main index DataFrame containing temporal struct arrays
    * @return Set of filenames containing the latest version of any matching value
    */
  private def locateFilesWithTemporal(
      column: String,
      values: Array[Any],
      df: DataFrame
  ): Set[String] = {
    val allExploded = loadTemporalColumnIndex(df, column)

    // Filter to requested values
    val filtered = allExploded.where(col("_value").isin(values: _*))

    // For each value, keep only the file with the latest timestamp
    val w = Window.partitionBy("_value").orderBy(col("_max_ts").desc_nulls_last)
    val pruned = filtered
      .withColumn("_rank", row_number().over(w))
      .filter(col("_rank") === 1)
      .select("filename")
      .distinct()

    collectFilenamesViaStaging(pruned)
  }

  /** Locates files using range indexes by checking if any query value falls
    * within the file's [min, max] range.
    *
    * Builds an OR condition across all values, checking file_min <= value AND
    * file_max >= value for each. Logs a warning when value count exceeds 1000.
    *
    * @param column The range index column name
    * @param values Array of values to check against per-file min/max ranges
    * @param indexDf The main index DataFrame containing range struct columns
    * @return Set of filenames whose stored range contains at least one query value
    */
  private def locateFilesWithRange(
      column: String,
      values: Array[Any],
      indexDf: DataFrame
  ): Set[String] = {
    val rangeCol = s"range_${column}"
    if (!indexDf.columns.contains(rangeCol)) {
      logger.warn(s"Range column $rangeCol not found in index, returning empty result")
      Set.empty
    } else if (values.isEmpty) {
      Set.empty
    } else {
      logger.warn(s"Range query on column '$column' with ${values.length} values")
      if (values.length > 1000) {
        logger.warn(s"Range query has ${values.length} values; large OR expression may be slow")
      }

      // Use Spark lit() for type-safe comparisons instead of Comparable casts.
      // This handles type coercion (e.g. Long vs Int) and NaN correctly.
      val valueConditions = values.map { v =>
        col("file_min") <= lit(v) && col("file_max") >= lit(v)
      }

      val matchingFiles = indexDf
        .select(
          col("filename"),
          col(s"$rangeCol.min").alias("file_min"),
          col(s"$rangeCol.max").alias("file_max")
        )
        .where(col("file_min").isNotNull && col("file_max").isNotNull)
        .where(valueConditions.reduce(_ || _))
        .select("filename")
        .distinct()

      collectFilenamesViaStaging(matchingFiles)
    }
  }

  /** Locates files based on a DataFrame containing join column values. Handles
    * both regular indexes and bloom filter indexes.
    *
    * @param valuesDf
    *   DataFrame containing the distinct values to search for
    * @param columnMappings
    *   Map from join column names to storage column names in the index
    * @param joinColumns
    *   The columns to use for filtering
    * @return
    *   A set of file names matching the criteria.
    *
    * @example
    * {{{
    * val files = index.locateFilesFromDataFrame(lookupDf, Seq("userId"))
    * }}}
    */
  def locateFilesFromDataFrame(
      valuesDf: DataFrame,
      columnMappings: Map[String, String],
      joinColumns: Seq[String]
  ): Set[String] = {
    require(valuesDf != null, "DataFrame must not be null")
    require(columnMappings != null && columnMappings.nonEmpty, "columnMappings must not be null or empty")
    val locateStart = System.currentTimeMillis()
    logger.warn(s"locateFilesFromDataFrame: querying columns ${joinColumns.mkString(", ")}")
    index match {
      case Some(indexDf) =>
        if (debugEnabled) {
          logger.warn(s"[debug] locateFilesFromDataFrame started: joinColumns=${joinColumns.mkString(",")}")
        }
        val bloomColumnSet = bloomColumns
        val temporalColumnSet = metadata.temporal_indexes.asScala.map(_.column).toSet
        val rangeColumnSet = metadata.range_indexes.asScala.map(_.column).toSet

        // Separate bloom, temporal, range, and regular columns
        val (bloomJoinColumns, nonBloomColumns) = joinColumns.partition {
          joinColumn => bloomColumnSet.contains(joinColumn)
        }
        val (temporalJoinColumns, nonTemporalColumns) = nonBloomColumns.partition {
          joinColumn => temporalColumnSet.contains(joinColumn)
        }
        val (rangeJoinColumns, regularJoinColumns) = nonTemporalColumns.partition {
          joinColumn => rangeColumnSet.contains(joinColumn)
        }

        // Get files from bloom filters
        val bloomStart = System.currentTimeMillis()
        val bloomFiles = if (bloomJoinColumns.nonEmpty) {
          bloomJoinColumns.map { joinColumn =>
            locateFilesWithBloomFromDataFrame(joinColumn, valuesDf, indexDf)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }
        val bloomMs = System.currentTimeMillis() - bloomStart

        // Get files from temporal indexes (pruned to latest timestamp per value)
        val temporalStart = System.currentTimeMillis()
        val temporalFiles = if (temporalJoinColumns.nonEmpty) {
          val repartitionedIndex = maybeRepartition(indexDf)
          temporalJoinColumns.map { joinColumn =>
            locateFilesWithTemporalFromDataFrame(joinColumn, valuesDf, repartitionedIndex)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }
        val temporalMs = System.currentTimeMillis() - temporalStart

        // Get files from range indexes
        val rangeStart = System.currentTimeMillis()
        val rangeFiles = if (rangeJoinColumns.nonEmpty) {
          rangeJoinColumns.map { joinColumn =>
            locateFilesWithRangeFromDataFrame(joinColumn, valuesDf, indexDf)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }
        val rangeMs = System.currentTimeMillis() - rangeStart

        // Get files from regular indexes
        val regularStart = System.currentTimeMillis()
        val regularFiles = if (regularJoinColumns.nonEmpty) {
          // Repartition the index DataFrame before explode to reduce
          // per-executor memory pressure on large indexes
          val repartitionedIndex = maybeRepartition(indexDf)
          if (debugEnabled) {
            logger.warn(s"[debug] locateFiles: index repartitioned")
          }

          val perColumnDFs = regularJoinColumns.map { joinColumn =>
            val storageColumn = columnMappings(joinColumn)
            val distinctValues = valuesDf.select(col(joinColumn)).distinct()
            val bloomCandidates = getAutoBloomCandidatesFromDf(storageColumn, joinColumn, valuesDf, indexDf)
            loadColumnIndex(repartitionedIndex, storageColumn, bloomCandidates)
              .join(
                distinctValues.withColumnRenamed(joinColumn, storageColumn),
                Seq(storageColumn),
                "leftsemi"
              )
              .select("filename")
              .distinct()
          }

          val intersectedDF = if (perColumnDFs.size == 1) {
            perColumnDFs.head
          } else {
            perColumnDFs.reduce((a, b) => a.join(b, Seq("filename"), "inner"))
          }

          if (debugEnabled) {
            logger.warn(s"[debug] locateFiles: about to collectFilenamesViaStaging at ${System.currentTimeMillis() - locateStart}ms")
          }
          collectFilenamesViaStaging(intersectedDF)
        } else {
          Set.empty[String]
        }
        val regularMs = System.currentTimeMillis() - regularStart

        // Combine results - intersect across types that returned results (AND semantics)
        // Track which categories were queried (had input), not just which returned results
        val queriedResults: Seq[Set[String]] = Seq(
          if (bloomJoinColumns.nonEmpty) Some(bloomFiles) else None,
          if (temporalJoinColumns.nonEmpty) Some(temporalFiles) else None,
          if (rangeJoinColumns.nonEmpty) Some(rangeFiles) else None,
          if (regularJoinColumns.nonEmpty) Some(regularFiles) else None
        ).flatten

        val allFiles = if (queriedResults.isEmpty) {
          Set.empty[String]
        } else {
          queriedResults.reduce(_ intersect _)
        }
        val totalMs = System.currentTimeMillis() - locateStart
        // Add per-type timing (#38) to match locateFiles
        logger.warn(
          s"locateFilesFromDataFrame: ${allFiles.size} files matched in ${totalMs}ms " +
            s"(bloom=${bloomMs}ms, temporal=${temporalMs}ms, range=${rangeMs}ms, regular=${regularMs}ms)"
        )
        if (allFiles.isEmpty) Set.empty else allFiles
      case None =>
        // Log when index table is None (#33)
        logger.warn(s"locateFilesFromDataFrame: no index table found for index '$name', returning empty result")
        Set.empty
    }
  }

  /** Locates files using temporal indexes from a DataFrame of values, pruning
    * to only files containing the latest version of each value.
    *
    * Joins the exploded temporal index with distinct query values, then applies
    * a window function to keep only the file with the highest max_ts per value.
    * This ensures joins against temporal indexes always use the most recent data.
    *
    * @param column The temporal index value column name
    * @param valuesDf DataFrame containing a column named `column` with values to search for
    * @param indexDf The main index DataFrame (should already be repartitioned if needed)
    * @return Set of filenames containing the latest version of any matching value
    */
  private def locateFilesWithTemporalFromDataFrame(
      column: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Set[String] = {
    val allExploded = loadTemporalColumnIndex(indexDf, column)

    // Join with query values
    val distinctValues = valuesDf.select(col(column)).distinct()
      .withColumnRenamed(column, "_value")
    val matched = allExploded.join(distinctValues, Seq("_value"), "inner")

    // For each value, keep only the file with the latest timestamp
    val w = Window.partitionBy("_value").orderBy(col("_max_ts").desc_nulls_last)
    val pruned = matched
      .withColumn("_rank", row_number().over(w))
      .filter(col("_rank") === 1)
      .select("filename")
      .distinct()

    val result = collectFilenamesViaStaging(pruned)
    // Add result logging (#36)
    logger.warn(s"Temporal DF query on column '$column': ${result.size} files matched")
    result
  }

  /** Locates files using range indexes from a DataFrame of values.
    *
    * Collects up to 10,000 distinct query values. For sets exceeding 1,000 values,
    * falls back to a bounding-box optimization (query min/max vs file min/max).
    * For smaller sets, checks per-value containment for precise pruning.
    *
    * @note Distinct values are truncated to 10,000 to avoid driver OOM.
    *   For value sets exceeding 1,000, a bounding-box optimization is used
    *   which may include false-positive files whose ranges overlap but contain
    *   no actual matching values.
    *
    * @param column The range index column name
    * @param valuesDf DataFrame containing a column named `column` with values to search for
    * @param indexDf The main index DataFrame containing range struct columns
    * @return Set of filenames whose stored range overlaps with the query values
    */
  private def locateFilesWithRangeFromDataFrame(
      column: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Set[String] = {
    val rangeCol = s"range_${column}"
    if (!indexDf.columns.contains(rangeCol)) {
      Set.empty
    } else {
      // Collect distinct query values (bounded to avoid driver OOM)
      val rawCount = valuesDf.select(col(column)).distinct().count()
      if (rawCount > 10000) {
        logger.warn(s"Range query on '$column': truncating $rawCount distinct values to 10000 for per-value pruning")
      }
      val distinctValues = valuesDf
        .select(col(column))
        .distinct()
        .limit(10000)
        .collect()
        .map(_.get(0))
        .filter(_ != null)

      if (distinctValues.isEmpty) {
        Set.empty
      } else {
        logger.warn(s"Range query on column '$column' with ${distinctValues.length} distinct values")

        if (distinctValues.length > 1000) {
          logger.warn(s"Range query: large value set (${distinctValues.length}), using bounding box optimization")
          val minMaxRow = valuesDf.agg(
            min(col(column)).alias("q_min"),
            max(col(column)).alias("q_max")
          ).head()

          if (minMaxRow.isNullAt(0) || minMaxRow.isNullAt(1)) {
            logger.warn(s"Range query for '$column': min/max values are null after aggregation, returning empty result")
            Set.empty[String]
          } else {
            val matchingFiles = indexDf
              .select(
                col("filename"),
                col(s"$rangeCol.min").alias("file_min"),
                col(s"$rangeCol.max").alias("file_max")
              )
              .where(col("file_min").isNotNull && col("file_max").isNotNull)
              .where(col("file_max") >= lit(minMaxRow.get(0)) && col("file_min") <= lit(minMaxRow.get(1)))
              .select("filename")
              .distinct()

            collectFilenamesViaStaging(matchingFiles)
          }
        } else {
          // For reasonable value sets, check per-value containment (precise pruning)
          val valueConditions = distinctValues.map { v =>
            col("file_min") <= lit(v) && col("file_max") >= lit(v)
          }

          val matchingFiles = indexDf
            .select(
              col("filename"),
              col(s"$rangeCol.min").alias("file_min"),
              col(s"$rangeCol.max").alias("file_max")
            )
            .where(col("file_min").isNotNull && col("file_max").isNotNull)
            .where(valueConditions.reduce(_ || _))
            .select("filename")
            .distinct()

          collectFilenamesViaStaging(matchingFiles)
        }
      }
    }
  }

  /** Returns a DataFrame of per-column index statistics and total file count.
    *
    * For each indexed column, computes statistics on the array length (number of
    * distinct values per file): min, max, avg, median, and standard deviation.
    * Also includes the total number of indexed files.
    *
    * @return Single-row DataFrame with FileCount and per-column stat structs,
    *         or an empty DataFrame if no index exists
    *
    * @example
    * {{{
    * index.stats()  // prints index statistics to console
    * }}}
    */
  def stats(): DataFrame = {
    logger.warn(s"Computing stats for index '$name'")
    index match {
      case Some(df) =>
        // File count
        val fileCountAgg = df.select(countDistinct("filename").as("FileCount"))

        // For each index, compute stats as a struct column
        val statCols = storageColumns.toSeq.map { colName =>
          val lenCol = size(col(colName))
          struct(
            min(lenCol).as("min"),
            max(lenCol).as("max"),
            avg(lenCol).as("avg"),
            expr("percentile_approx(size(" + colName + "), 0.5)").as("median"),
            stddev(lenCol).as("stddev")
          ).as(colName)
        }

        // Build a single-row DataFrame with all stats
        val aggExprs =
          Seq(countDistinct("filename").as("FileCount")) ++ statCols
        df.agg(aggExprs.head, aggExprs.tail: _*)

      case None =>
        spark.emptyDataFrame
    }
  }

  /** Prints the index DataFrame to the console for debugging.
    *
    * Displays the contents and schema of the main index Delta table.
    * This is an internal/diagnostic method.
    *
    * @param truncate Whether to truncate long values in the display (default: false)
    */
  private[ariadne] def printIndex(truncate: Boolean = false): Unit = {
    index match {
      case Some(df) =>
        df.show(truncate)
        df.printSchema()
      case None =>
        logger.warn(s"No index data found for index '$name'")
    }
  }

  /** Prints the metadata associated with the index to the console.
    *
    * This metadata contains details about the index, including its schema,
    * format, and tracked files.
    */
  private[ariadne] def printMetadata: Unit = println(metadata)
}
