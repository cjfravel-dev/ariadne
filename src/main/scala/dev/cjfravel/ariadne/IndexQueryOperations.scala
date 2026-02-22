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
    delta(indexFilePath) match {
      case Some(delta) => {
        val index = delta.toDF
        // search largeIndexesFilePath for consolidated column tables to merge into the index
        if (!exists(largeIndexesFilePath)) {
          return Some(index)
        }

        val consolidatedTables = fs
          .listStatus(largeIndexesFilePath)
          .filter(_.isDirectory)
          .map(_.getPath)
          .filter(path =>
            exists(path) && DeltaTable.isDeltaTable(spark, path.toString)
          )
          .toSeq

        // for each consolidated column table, read and merge into the index
        val combined = consolidatedTables.foldLeft(index) {
          (accumDf, columnPath) =>
            val colName = columnPath.getName
            val safeColName = s"ariadne_large_index_$colName"

            try {
              val largeIndex = spark.read
                .format("delta")
                .load(columnPath.toString)
                .groupBy("filename")
                .agg(collect_set(colName).alias(safeColName))
                .select("filename", safeColName)

              accumDf
                .join(largeIndex, Seq("filename"), "left")
                .withColumn(colName, coalesce(col(colName), col(safeColName)))
                .drop(safeColName)
            } catch {
              case _: Exception =>
                // If there's an issue reading the large index table, continue without it
                accumDf
            }
        }

        Some(combined)
      }
      case None => None
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
        // Separate bloom, temporal, and regular index queries
        val bloomColumnSet = bloomColumns
        val temporalColumnSet = metadata.temporal_indexes.asScala.map(_.column).toSet
        val (bloomQueries, nonBloomQueries) = indexes.partition {
          case (col, _) => bloomColumnSet.contains(col)
        }
        val (temporalQueries, regularQueries) = nonBloomQueries.partition {
          case (col, _) => temporalColumnSet.contains(col)
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

        // Get files from regular indexes
        val regularFiles = if (regularQueries.nonEmpty) {
          locateFilesRegular(regularQueries, df)
        } else {
          Set.empty[String]
        }

        // Combine results (union / OR semantics)
        val allFiles = bloomFiles.union(temporalFiles).union(regularFiles)
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
    val schema = StructType(
      Array(StructField("filename", StringType, nullable = false))
    )
    val emptyDF =
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        schema
      )

    val resultDF = indexes.foldLeft(emptyDF) {
      case (accumDF, (column, values)) =>
        val filteredDF = df
          .select("filename", column)
          .withColumn("value", explode(col(column)))
          .where(col("value").isin(values: _*))
          .select("filename")
          .distinct

        accumDF.union(filteredDF)
    }

    collectFilenamesViaStaging(resultDF)
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
    // Explode the struct array: Array[Struct(value, max_ts)] → rows of (filename, value, max_ts)
    val exploded = df
      .select(col("filename"), explode(col(column)).alias("_temporal"))
      .select(
        col("filename"),
        col("_temporal.value").alias("_value"),
        col("_temporal.max_ts").alias("_max_ts")
      )

    // Filter to requested values
    val filtered = exploded.where(col("_value").isin(values: _*))

    // For each value, keep only the file with the latest timestamp
    val w = Window.partitionBy("_value").orderBy(col("_max_ts").desc_nulls_last)
    val pruned = filtered
      .withColumn("_rank", row_number().over(w))
      .filter(col("_rank") === 1)
      .select("filename")
      .distinct()

    collectFilenamesViaStaging(pruned)
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

        // Separate bloom, temporal, and regular columns
        val (bloomJoinColumns, nonBloomColumns) = joinColumns.partition {
          joinColumn => bloomColumnSet.contains(joinColumn)
        }
        val (temporalJoinColumns, regularJoinColumns) = nonBloomColumns.partition {
          joinColumn => temporalColumnSet.contains(joinColumn)
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

        // Get files from regular indexes
        val regularFiles = if (regularJoinColumns.nonEmpty) {
          val schema = StructType(
            Array(StructField("filename", StringType, nullable = false))
          )
          val emptyDF =
            spark.createDataFrame(
              spark.sparkContext.emptyRDD[Row],
              schema
            )

          // Repartition the index DataFrame before explode to reduce
          // per-executor memory pressure on large indexes
          val repartitionedIndex = maybeRepartition(indexDf)
          if (debugEnabled) {
            logger.warn(s"[debug] locateFiles: index repartitioned")
          }

          val resultDF = regularJoinColumns.foldLeft(emptyDF) {
            (accumDF, joinColumn) =>
              val storageColumn = columnMappings(joinColumn)

              // Create a DataFrame with distinct values for this column
              val distinctValues = valuesDf.select(col(joinColumn)).distinct()

              // Explode the index array column and join with the values
              val filteredDF = repartitionedIndex
                .select(
                  col("filename"),
                  explode(col(storageColumn)).alias("value")
                )
                .join(
                  distinctValues.withColumnRenamed(joinColumn, "value"),
                  Seq("value"),
                  "leftsemi"
                )
                .select("filename")
                .distinct()

              accumDF.union(filteredDF)
          }

          if (debugEnabled) {
            logger.warn(s"[debug] locateFiles: about to collectFilenamesViaStaging at ${System.currentTimeMillis() - locateStart}ms")
          }
          collectFilenamesViaStaging(resultDF)
        } else {
          Set.empty[String]
        }

        // Combine results (union / OR semantics)
        val allFiles = bloomFiles.union(temporalFiles).union(regularFiles)
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
    // Explode the struct array: Array[Struct(value, max_ts)] → rows of (filename, value, max_ts)
    val exploded = indexDf
      .select(col("filename"), explode(col(column)).alias("_temporal"))
      .select(
        col("filename"),
        col("_temporal.value").alias("_value"),
        col("_temporal.max_ts").alias("_max_ts")
      )

    // Join with query values
    val distinctValues = valuesDf.select(col(column)).distinct()
      .withColumnRenamed(column, "_value")
    val matched = exploded.join(distinctValues, Seq("_value"), "inner")

    // For each value, keep only the file with the latest timestamp
    val w = Window.partitionBy("_value").orderBy(col("_max_ts").desc_nulls_last)
    val pruned = matched
      .withColumn("_rank", row_number().over(w))
      .filter(col("_rank") === 1)
      .select("filename")
      .distinct()

    collectFilenamesViaStaging(pruned)
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
