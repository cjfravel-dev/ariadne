package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import io.delta.tables.DeltaTable
import scala.collection.JavaConverters._

/** Trait providing index building operations for Index instances.
  */
trait IndexBuildOperations extends IndexFileOperations {
  self: Index =>

  /** Hadoop root path for large index delta tables */
  protected def largeIndexesFilePath: Path =
    new Path(storagePath, "large_indexes")

  /** Hadoop path for the index delta table */
  protected def indexFilePath: Path = new Path(storagePath, "index")

  /** Helper function to get the storage column names for internal use.
    *
    * @return
    *   Set of column names used for internal storage
    */
  protected def storageColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.array_column).toSet

  /** Builds regular indexes DataFrame from the base data.
    *
    * @param df The base DataFrame with filename column
    * @return DataFrame with regular indexes aggregated by filename
    */
  protected def buildRegularIndexes(df: DataFrame): DataFrame = {
    val regularIndexes = metadata.indexes.asScala.toSet ++ metadata.computed_indexes.keySet().asScala
    
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

  /** Handles large indexes by splitting and storing them separately.
    *
    * @param df The DataFrame to process for large indexes
    */
  protected def handleLargeIndexes(df: DataFrame): Unit = {
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
    
    val largeFiles = largeGroupedDf
      .select("filename")
      .where(allStorageColumns.map(colName => col(colName).isNotNull).reduce(_ && _))
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSet

    // Store large files separately
    largeFiles.foreach { fileName =>
      allStorageColumns.foreach(colName => {
        val filePath =
          new Path(
            new Path(largeIndexesFilePath, colName),
            IndexPathUtils.cleanFileName(fileName)
          )
        val indexDf = largeGroupedDf
          .where(col("filename") === fileName)
          .select("filename", colName)
          .withColumn(colName, explode(col(colName)))

        if (indexDf.count() > 0) {
          indexDf.write
            .format("delta")
            .mode("overwrite")
            .save(filePath.toString)
        }
      })
    }
  }

  /** Merges the processed DataFrame to Delta table storage.
    *
    * @param df The DataFrame to merge/write to Delta
    */
  protected def mergeToDelta(df: DataFrame): Unit = {
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

    // Write to Delta
    if (allStorageColumns.nonEmpty) {
      delta(indexFilePath) match {
        case Some(delta) =>
          delta
            .as("target")
            .merge(
              smallGroupedDf.as("source"),
              "target.filename = source.filename"
            )
            .whenMatched()
            .updateExpr(
              allStorageColumns.map(colName => colName -> s"source.$colName").toMap
            )
            .whenNotMatched()
            .insertAll()
            .execute()
        case None =>
          smallGroupedDf.write
            .format("delta")
            .mode("overwrite")
            .save(indexFilePath.toString)
      }
    } else {
      // If no storage columns, just ensure the filename tracking works
      smallGroupedDf.write
        .format("delta")
        .mode("overwrite")
        .save(indexFilePath.toString)
    }
  }

  
}