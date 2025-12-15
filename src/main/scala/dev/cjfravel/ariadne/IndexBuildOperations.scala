package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import io.delta.tables.DeltaTable
import scala.collection.JavaConverters._

/** Trait providing index building operations for Index instances.
  */
trait IndexBuildOperations extends BloomFilterOperations {
  self: Index =>

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
      metadata.exploded_field_indexes.asScala.map(_.array_column).toSet

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
    val withFilename = withComputedIndexes.withColumn("filename", input_file_name())
    
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

  /** Handles large indexes by appending directly to large_indexes/{column}.
    *
    * @param df The DataFrame to process for large indexes
    */
  protected def handleLargeIndexes(df: DataFrame): Unit = {
    val allStorageColumns = storageColumns
    if (allStorageColumns.isEmpty) return
    
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
      .mode("append")
      .save(stagingFilePath.toString)
  }

  /** Appends large index data directly to large_indexes/{column} Delta tables.
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
        columnData.write
          .format("delta")
          .mode("append")
          .save(columnPath.toString)
      }
    }
  }

  /** Consolidates staged data into the main index table. */
  protected def consolidateStaging(): Unit = {
    logger.warn("Starting consolidation of staged data")
    consolidateMainStaging()
    
    logger.warn("Consolidation complete")
  }

  /** Consolidates the main staging table into the main index. */
  private def consolidateMainStaging(): Unit = {
    if (!exists(stagingFilePath)) {
      logger.warn("No staging data to consolidate for main index")
      return
    }
    
    val allStorageColumns = storageColumns
    val stagingDf = spark.read.format("delta").load(stagingFilePath.toString)
    
    if (allStorageColumns.nonEmpty) {
      delta(indexFilePath) match {
        case Some(deltaTable) =>
          logger.warn(s"Merging staging data into main index at ${indexFilePath}")
          deltaTable
            .as("target")
            .merge(
              stagingDf.as("source"),
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