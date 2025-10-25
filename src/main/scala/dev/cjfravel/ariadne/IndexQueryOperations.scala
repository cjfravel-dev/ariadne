package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
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
          .filter(path => exists(path) && DeltaTable.isDeltaTable(spark, path.toString))
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

  /** Locates files based on a DataFrame containing join column values.
    * Uses broadcast joins for efficient filtering when value sets are large.
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
    import org.apache.spark.sql.functions.broadcast
    
    index match {
      case Some(indexDf) =>
        val schema = StructType(
          Array(StructField("filename", StringType, nullable = false))
        )
        val emptyDF =
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[Row],
            schema
          )
        
        // For each join column, find files that match and union them
        // This matches the logic of the original locateFiles method
        val resultDF = joinColumns.foldLeft(emptyDF) { (accumDF, joinColumn) =>
          val storageColumn = columnMappings(joinColumn)
          
          // Create a DataFrame with distinct values for this column
          val distinctValues = valuesDf.select(col(joinColumn)).distinct()
          
          // Explode the index array column and join with the values
          val filteredDF = indexDf
            .select(col("filename"), explode(col(storageColumn)).alias("value"))
            .join(
              broadcast(distinctValues.withColumnRenamed(joinColumn, "value")),
              Seq("value"),
              "leftsemi"
            )
            .select("filename")
            .distinct()
          
          accumDF.union(filteredDF)
        }
        
        resultDF.distinct().collect().map(_.getString(0)).toSet
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