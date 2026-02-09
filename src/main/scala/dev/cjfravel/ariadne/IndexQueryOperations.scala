package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

/** Trait providing query operations for Index instances.
  */
trait IndexQueryOperations extends IndexJoinOperations {
  self: Index =>

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
        // Separate bloom and regular index queries
        val bloomColumnSet = bloomColumns
        val (bloomQueries, regularQueries) = indexes.partition {
          case (col, _) => bloomColumnSet.contains(col)
        }

        // Get files from bloom filters
        val bloomFiles = if (bloomQueries.nonEmpty) {
          bloomQueries.flatMap { case (col, values) =>
            locateFilesWithBloom(col, values, df)
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

        // Combine results
        if (bloomFiles.isEmpty && regularFiles.isEmpty) {
          Set.empty
        } else if (bloomFiles.isEmpty) {
          regularFiles
        } else if (regularFiles.isEmpty) {
          bloomFiles
        } else {
          // Both have results - use union (OR semantics) to match existing behavior
          bloomFiles.union(regularFiles)
        }
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
    val tempPath = new Path(
      IndexPathUtils.tempPath,
      s"query_files_${System.currentTimeMillis()}_${java.util.UUID.randomUUID()}"
    )

    try {
      // Write distinct filenames to temp location (distributed operation)
      resultDF
        .distinct()
        .write
        .mode("overwrite")
        .parquet(tempPath.toString)

      logger.debug(s"Staged filenames to $tempPath")

      // Read back from temp (simple, optimized operation)
      spark.read
        .parquet(tempPath.toString)
        .select("filename")
        .collect()
        .map(_.getString(0))
        .toSet
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

  /** Locates files based on a DataFrame containing join column values. Uses
    * broadcast joins for efficient filtering when value sets are large. Handles
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
    index match {
      case Some(indexDf) =>
        val bloomColumnSet = bloomColumns

        // Separate bloom and regular columns
        val (bloomJoinColumns, regularJoinColumns) = joinColumns.partition {
          joinColumn =>
            bloomColumnSet.contains(joinColumn)
        }

        // Get files from bloom filters
        val bloomFiles = if (bloomJoinColumns.nonEmpty) {
          bloomJoinColumns.flatMap { joinColumn =>
            locateFilesWithBloomFromDataFrame(joinColumn, valuesDf, indexDf)
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

          val resultDF = regularJoinColumns.foldLeft(emptyDF) {
            (accumDF, joinColumn) =>
              val storageColumn = columnMappings(joinColumn)

              // Create a DataFrame with distinct values for this column
              val distinctValues = valuesDf.select(col(joinColumn)).distinct()

              // Explode the index array column and join with the values
              // Note: Removed explicit broadcast hint to let Spark optimizer decide join strategy
              val filteredDF = indexDf
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

          collectFilenamesViaStaging(resultDF)
        } else {
          Set.empty[String]
        }

        // Combine results
        if (bloomFiles.isEmpty && regularFiles.isEmpty) {
          Set.empty
        } else if (bloomFiles.isEmpty) {
          regularFiles
        } else if (regularFiles.isEmpty) {
          bloomFiles
        } else {
          bloomFiles.union(regularFiles)
        }
      case None => Set()
    }
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
