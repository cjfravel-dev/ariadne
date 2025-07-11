package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._
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
        // search largeIndexesFilePath for files to merge into the index
        if (!exists(largeIndexesFilePath)) {
          return Some(index)
        }

        val largeIndexesFiles = fs
          .listStatus(largeIndexesFilePath)
          .filter(_.isDirectory)
          .flatMap { dir =>
            fs.listStatus(dir.getPath).map(_.getPath.toString)
          }
          .toSet

        // for each file in largeIndexesFiles, read the file and merge it into the index
        val combined = largeIndexesFiles.foldLeft(index) {
          (accumDf, fileName) =>
            val colName = fileName.split("/").reverse(1)
            val safeColName = s"ariadne_large_index_$colName"
            val largeIndex = spark.read
              .format("delta")
              .load(fileName)
              .groupBy("filename")
              .agg(collect_set(colName).alias(safeColName))
              .select("filename", safeColName)

            accumDf
              .join(largeIndex, Seq("filename"), "left")
              .withColumn(colName, coalesce(col(colName), col(safeColName)))
              .drop(safeColName)
        }

        Some(combined)
      }
      case None => None
    }
  }

  /** Locates files based on index values.
    * @param indexes
    *   A map of index column names to their values.
    * @return
    *   A set of file names matching the criteria.
    */
  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
    index match {
      case Some(df) =>
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

        resultDF.distinct.collect.map(_.getString(0)).toSet
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