package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

/** Trait providing query operations for Index instances.
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
    if (!exists(largeIndexesFilePath)) return Set.empty
    fs.listStatus(largeIndexesFilePath)
      .filter(_.isDirectory)
      .map(_.getPath)
      .filter(path =>
        exists(path) && DeltaTable.isDeltaTable(spark, path.toString)
      )
      .map(_.getName)
      .toSet
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
      case _: Exception => None
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

  /** Locates files based on index values. Handles both regular indexes (using
    * explode) and bloom filter indexes (using probabilistic matching).
    *
    * @param indexes
    *   A map of index column names to their values.
    * @return
    *   A set of file names matching the criteria.
    */
  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
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
        val bloomFiles = if (bloomQueries.nonEmpty) {
          bloomQueries.flatMap { case (col, values) =>
            locateFilesWithBloom(col, values, df)
          }.toSet
        } else {
          Set.empty[String]
        }

        // Get files from temporal indexes (pruned to latest timestamp per value)
        val temporalFiles = if (temporalQueries.nonEmpty) {
          temporalQueries.flatMap { case (column, values) =>
            locateFilesWithTemporal(column, values, df)
          }.toSet
        } else {
          Set.empty[String]
        }

        // Get files from range indexes
        val rangeFiles = if (rangeQueries.nonEmpty) {
          rangeQueries.map { case (column, values) =>
            locateFilesWithRange(column, values, df)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }

        // Get files from regular indexes
        val regularFiles = if (regularQueries.nonEmpty) {
          locateFilesRegular(regularQueries, df)
        } else {
          Set.empty[String]
        }

        // Combine results - intersect across types that returned results (AND semantics)
        val nonEmptyResults = Seq(bloomFiles, temporalFiles, rangeFiles, regularFiles).filter(_.nonEmpty)
        val allFiles = if (nonEmptyResults.isEmpty) {
          Set.empty[String]
        } else {
          nonEmptyResults.reduce(_ intersect _)
        }
        if (allFiles.isEmpty) Set.empty else allFiles
      case None => Set()
    }
  }

  /** Collects filenames via staging through temp storage. This separates the
    * distinct operation from collect to avoid executor failures on large result
    * sets.
    *
    * @param resultDF
    *   DataFrame containing filename column to collect
    * @return
    *   Set of distinct filenames
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

      // Get count before collect for debugging
      val fileCount = stagedFiles.count()
      logger.warn(s"Collecting $fileCount distinct filenames from staging")

      val files = stagedFiles
        .collect()
        .map(_.getString(0))
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

  /** Gets candidate files from auto-bloom filter for a column.
    * Returns None if no auto-bloom is available for the column.
    * Files with null bloom filters are included as candidates (backward compat).
    */
  protected def getAutoBloomCandidates(
      column: String,
      values: Array[Any],
      indexDf: DataFrame
  ): Option[Set[String]] = {
    if (!metadata.auto_bloom_indexes.asScala.contains(column)) return None
    val autoBloomCol = s"auto_bloom_${column}"
    if (!indexDf.columns.contains(autoBloomCol)) return None

    val candidates = indexDf
      .select("filename", autoBloomCol)
      .collect()
      .filter { row =>
        val bloomBytes = row.getAs[Array[Byte]](autoBloomCol)
        if (bloomBytes == null) true
        else values.exists(v => bloomMightContain(bloomBytes, v))
      }
      .map(_.getString(0))
      .toSet

    logger.warn(s"Auto-bloom filter for $column: ${candidates.size} candidate files")
    Some(candidates)
  }

  /** Gets candidate files from auto-bloom filter using values from a DataFrame. */
  protected def getAutoBloomCandidatesFromDf(
      storageColumn: String,
      joinColumn: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Option[Set[String]] = {
    if (!metadata.auto_bloom_indexes.asScala.contains(storageColumn)) return None
    val autoBloomCol = s"auto_bloom_${storageColumn}"
    if (!indexDf.columns.contains(autoBloomCol)) return None

    val values = valuesDf
      .select(joinColumn)
      .distinct()
      .collect()
      .map(_.get(0))
      .filter(_ != null)

    if (values.isEmpty) return None
    getAutoBloomCandidates(storageColumn, values, indexDf)
  }

  /** Locates files using regular (non-bloom) indexes.
    *
    * @param indexes
    *   A map of index column names to their values.
    * @param df
    *   The index DataFrame.
    * @return
    *   A set of file names matching the criteria.
    */
  private def locateFilesRegular(
      indexes: Map[String, Array[Any]],
      df: DataFrame
  ): Set[String] = {
    if (indexes.isEmpty) return Set.empty

    val perColumnFiles = indexes.map { case (column, values) =>
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

    collectFilenamesViaStaging(intersectedDF)
  }

  /** Locates files using temporal indexes, pruning to only files containing
    * the latest version of each value (by max timestamp).
    *
    * @param column The temporal index value column name
    * @param values Values to search for
    * @param df The index DataFrame
    * @return Set of filenames containing the latest versions
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
    * @param column The range index column name
    * @param values Values to search for
    * @param df The index DataFrame
    * @return Set of filenames whose range overlaps with query values
    */
  private def locateFilesWithRange(
      column: String,
      values: Array[Any],
      df: DataFrame
  ): Set[String] = {
    val rangeCol = s"range_${column}"
    if (!df.columns.contains(rangeCol)) return Set.empty

    df.select("filename", rangeCol)
      .where(col(rangeCol).isNotNull)
      .collect()
      .filter { row =>
        val rangeStruct = row.getStruct(1)
        if (rangeStruct == null || rangeStruct.isNullAt(0) || rangeStruct.isNullAt(1)) false
        else {
          val fileMin = rangeStruct.get(0).asInstanceOf[Comparable[Any]]
          val fileMax = rangeStruct.get(1).asInstanceOf[Comparable[Any]]
          values.exists { v =>
            fileMin.compareTo(v) <= 0 && fileMax.compareTo(v) >= 0
          }
        }
      }
      .map(_.getString(0))
      .toSet
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
    */
  def locateFilesFromDataFrame(
      valuesDf: DataFrame,
      columnMappings: Map[String, String],
      joinColumns: Seq[String]
  ): Set[String] = {
    val locateStart = System.currentTimeMillis()
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
        val bloomFiles = if (bloomJoinColumns.nonEmpty) {
          bloomJoinColumns.flatMap { joinColumn =>
            locateFilesWithBloomFromDataFrame(joinColumn, valuesDf, indexDf)
          }.toSet
        } else {
          Set.empty[String]
        }

        // Get files from temporal indexes (pruned to latest timestamp per value)
        val temporalFiles = if (temporalJoinColumns.nonEmpty) {
          val repartitionedIndex = maybeRepartition(indexDf)
          temporalJoinColumns.flatMap { joinColumn =>
            locateFilesWithTemporalFromDataFrame(joinColumn, valuesDf, repartitionedIndex)
          }.toSet
        } else {
          Set.empty[String]
        }

        // Get files from range indexes
        val rangeFiles = if (rangeJoinColumns.nonEmpty) {
          rangeJoinColumns.map { joinColumn =>
            locateFilesWithRangeFromDataFrame(joinColumn, valuesDf, indexDf)
          }.reduce(_ intersect _)
        } else {
          Set.empty[String]
        }

        // Get files from regular indexes
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

        // Combine results - intersect across types that returned results (AND semantics)
        val nonEmptyResults = Seq(bloomFiles, temporalFiles, rangeFiles, regularFiles).filter(_.nonEmpty)
        val allFiles = if (nonEmptyResults.isEmpty) {
          Set.empty[String]
        } else {
          nonEmptyResults.reduce(_ intersect _)
        }
        if (allFiles.isEmpty) Set.empty else allFiles
      case None => Set()
    }
  }

  /** Locates files using temporal indexes from a DataFrame of values, pruning
    * to only files containing the latest version of each value.
    *
    * @param column The temporal index value column name
    * @param valuesDf DataFrame containing values to search for
    * @param indexDf The index DataFrame
    * @return Set of filenames containing the latest versions
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

    collectFilenamesViaStaging(pruned)
  }

  /** Locates files using range indexes from a DataFrame of values.
    *
    * Computes the min/max of the incoming query values and filters index rows
    * where the file's stored range overlaps: file_max >= query_min AND file_min <= query_max.
    *
    * @param column The range index column name
    * @param valuesDf DataFrame containing values to search for
    * @param indexDf The index DataFrame
    * @return Set of filenames whose range overlaps with query values
    */
  private def locateFilesWithRangeFromDataFrame(
      column: String,
      valuesDf: DataFrame,
      indexDf: DataFrame
  ): Set[String] = {
    val rangeCol = s"range_${column}"
    if (!indexDf.columns.contains(rangeCol)) return Set.empty

    // Get query range (min/max of incoming values)
    val queryStats = valuesDf.select(
      min(col(column)).alias("q_min"),
      max(col(column)).alias("q_max")
    ).collect()(0)

    val qMin = queryStats.get(0)
    val qMax = queryStats.get(1)
    if (qMin == null || qMax == null) return Set.empty

    // Filter files where range overlaps
    val matchingFiles = indexDf
      .select(col("filename"), col(s"$rangeCol.min").alias("file_min"), col(s"$rangeCol.max").alias("file_max"))
      .where(col("file_min").isNotNull && col("file_max").isNotNull)
      .where(col("file_max") >= lit(qMin) && col("file_min") <= lit(qMax))
      .select("filename")
      .distinct()

    collectFilenamesViaStaging(matchingFiles)
  }

  /** Returns a DataFrame of statistics for each indexed column (based on array
    * length per file) and file count.
    */
  def stats(): DataFrame = {
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

  /** Prints the index DataFrame to the console, including its schema.
    *
    * This method retrieves the latest version of the index stored in Delta
    * Lake, displays its contents, and prints its schema.
    */
  private[ariadne] def printIndex(truncate: Boolean = false): Unit = {
    index match {
      case Some(df) =>
        df.show(truncate)
        df.printSchema()
      case None =>
    }
  }

  /** Prints the metadata associated with the index to the console.
    *
    * This metadata contains details about the index, including its schema,
    * format, and tracked files.
    */
  private[ariadne] def printMetadata: Unit = println(metadata)
}
