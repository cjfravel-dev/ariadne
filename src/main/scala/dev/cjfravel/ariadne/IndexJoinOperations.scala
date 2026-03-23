package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.JavaConverters._

/** Trait providing join operations between DataFrames and indexed data.
  *
  * Orchestrates the join workflow:
  * 1. Maps join columns to their storage column names
  * 2. Locates relevant files using the index (via [[IndexQueryOperations]])
  * 3. Reads the located data files
  * 4. Applies temporal deduplication if applicable
  * 5. Performs the final DataFrame join
  *
  * Supports inner, left, right, left_semi, left_anti, and other standard join types.
  */
trait IndexJoinOperations extends IndexBuildOperations {
  self: Index =>

  /** Maps join column names to their corresponding storage column names.
    *
    * Bloom filter columns are prefixed with the bloom prefix, range columns are
    * prefixed with "range_", exploded field columns are mapped to their backing
    * array column, and all other columns map to themselves.
    *
    * @param joinColumns
    *   The column names used in joins
    * @return
    *   Map from each join column name to its storage column name in the index
    */
  protected def mapJoinColumnsToStorage(
      joinColumns: Seq[String]
  ): Map[String, String] = {
    val bloomColumnSet = bloomColumns
    val rangeColumnSet = metadata.range_indexes.asScala.map(_.column).toSet

    joinColumns.map { joinCol =>
      if (bloomColumnSet.contains(joinCol)) {
        joinCol -> (bloomColumnPrefix + joinCol)
      } else if (rangeColumnSet.contains(joinCol)) {
        joinCol -> (s"range_${joinCol}")
      } else {
        // Check if this is an exploded field column
        val explodedMapping =
          metadata.exploded_field_indexes.asScala.find(_.as_column == joinCol)
        explodedMapping match {
          case Some(mapping) => joinCol -> mapping.array_column
          case None          => joinCol -> joinCol
        }
      }
    }.toMap
  }

  /** Locates and reads indexed data files relevant to the given DataFrame.
    *
    * Uses the index to identify which files contain matching values, then
    * reads those files into a lazy DataFrame. The actual row-level filtering
    * happens in the subsequent join in [[join]].
    *
    * @param df
    *   The DataFrame to match against the index.
    * @param usingColumns
    *   The columns used for the join.
    * @return
    *   A lazy DataFrame containing data from indexed files.
    */
  protected def joinDf(df: DataFrame, usingColumns: Seq[String]): DataFrame = {
    val joinStart = System.currentTimeMillis()
    def elapsed(): String = {
      val ms = System.currentTimeMillis() - joinStart
      if (ms > 60000) f"${ms / 60000}m ${(ms % 60000) / 1000}s"
      else if (ms > 1000) f"${ms / 1000.0}%.1fs"
      else s"${ms}ms"
    }

    if (debugEnabled) {
      logger.warn(s"[debug] joinDf started: index=${name}, usingColumns=${usingColumns.mkString(",")}")
      logger.warn(s"[debug] input df schema: ${df.schema.fieldNames.mkString(",")}")
    }

    // Validate that join columns are included in selected columns (if selection is active)
    // or exist in the schema (if no selection)
    getSelectedColumns match {
      case Some(selectedCols) =>
        val missingJoinCols = usingColumns.filterNot(selectedCols.contains)
        if (missingJoinCols.nonEmpty) {
          throw new ColumnNotFoundException(
            s"Join columns must be included in selected columns. Missing: ${missingJoinCols.mkString(", ")}"
          )
        }
      case None =>
        // No selection active, but still validate columns exist in schema or are available indexes
        val invalidJoinCols = usingColumns.filterNot { colName =>
          SchemaHelper.fieldExists(storedSchema, colName) || this.indexes
            .contains(colName)
        }
        if (invalidJoinCols.nonEmpty) {
          throw new ColumnNotFoundException(
            s"Join columns not found in schema or indexes: ${invalidJoinCols.mkString(", ")}"
          )
        }
    }

    // Map join columns to storage columns
    val columnMappings = mapJoinColumnsToStorage(usingColumns)

    // Include both regular storage columns and bloom/range storage columns
    val allStorageColumns = this.storageColumns ++ this.bloomStorageColumns ++ this.rangeStorageColumns
    val storageColumnsToUse =
      columnMappings.values.toSet.intersect(allStorageColumns)
    logger.warn(s"Found indexes for ${storageColumnsToUse.mkString(",")}")

    // Get values from the user DataFrame using join column names
    val joinColumnsToUse = usingColumns.filter(col =>
      columnMappings.contains(col) && storageColumnsToUse.contains(
        columnMappings(col)
      )
    )

    // Get values from the user DataFrame using join column names
    val filteredValuesDf = df.select(joinColumnsToUse.map(col): _*)

    // Use the new DataFrame-based method to locate files
    val files = locateFilesFromDataFrame(
      filteredValuesDf,
      columnMappings,
      joinColumnsToUse
    )
    logger.warn(s"Found ${files.size} files in index")

    // Log data pruning metrics using stored file sizes
    try {
      val totalIndexedSize = metadata.total_indexed_file_size
      if (totalIndexedSize > 0 && files.nonEmpty) {
        delta(indexFilePath).foreach { dt =>
          val indexDf = dt.toDF
          if (indexDf.columns.contains("file_size")) {
            val totalFiles = indexDf.count()
            import spark.implicits._
            val filesDf = files.toSeq.toDF("filename")
            val matchedSizeResult = indexDf
              .join(filesDf, Seq("filename"), "inner")
              .agg(sum("file_size"))
              .head()
            val matchedSize = if (matchedSizeResult.isNullAt(0)) 0L else matchedSizeResult.getLong(0)
            val totalGB = totalIndexedSize / (1024.0 * 1024.0 * 1024.0)
            val matchedGB = matchedSize / (1024.0 * 1024.0 * 1024.0)
            val savedPercent = if (totalIndexedSize > 0) ((totalIndexedSize - matchedSize) * 100.0 / totalIndexedSize).toInt else 0
            logger.warn(f"Index pruning: loaded ${files.size}%d of $totalFiles%d files ($matchedGB%.2f GB of $totalGB%.2f GB) — $savedPercent%%  data pruned")
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to compute pruning metrics: ${e.getMessage}")
    }

    if (debugEnabled) {
      logger.warn(s"[debug] locateFiles completed in ${elapsed()}, files: ${files.size}")
      try {
        val fileSizes = files.toSeq.map { f =>
          val path = new org.apache.hadoop.fs.Path(f)
          val fileFs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
          val size = fileFs.getFileStatus(path).getLen
          (f, size)
        }.sortBy(-_._2)
        val totalBytes = fileSizes.map(_._2).sum
        val totalMB = totalBytes / (1024.0 * 1024.0)
        val totalGB = totalBytes / (1024.0 * 1024.0 * 1024.0)
        logger.warn(f"[debug] total file size: $totalMB%.1fMB ($totalGB%.2fGB) across ${files.size} files")
        val avgMB = totalMB / files.size
        val maxFile = fileSizes.head
        val minFile = fileSizes.last
        val maxMB = maxFile._2 / (1024.0 * 1024.0)
        val minMB = minFile._2 / (1024.0 * 1024.0)
        logger.warn(f"[debug] file sizes: avg=$avgMB%.1fMB, max=$maxMB%.1fMB, min=$minMB%.1fMB")
        logger.warn(f"[debug] largest file: $maxMB%.1fMB -> ${maxFile._1}")
        logger.warn(f"[debug] smallest file: $minMB%.1fMB -> ${minFile._1}")
        // Log top 5 largest files
        fileSizes.take(5).foreach { case (f, size) =>
          val mb = size / (1024.0 * 1024.0)
          logger.warn(f"[debug]   $mb%.1fMB -> $f")
        }
      } catch {
        case e: Exception =>
          logger.warn(s"[debug] failed to get file sizes: ${e.getMessage}")
      }
    }

    // Read the data files located by the index.
    // repartitionDataFiles controls whether the data files are repartitioned.
    // Default is false — data files keep their natural parquet partitioning.
    // Enable when reading all columns from very large indexes to reduce
    // per-executor memory pressure.
    logger.warn(s"Reading ${files.size} data files from index '$name'")
    val rawReadIndex = if (repartitionDataFiles) {
      maybeRepartition(readFiles(files))
    } else {
      readFiles(files)
    }
    // Apply temporal deduplication if any temporal indexes are being used in this join
    val readIndex = applyTemporalDeduplication(rawReadIndex, usingColumns)
    if (debugEnabled) {
      logger.warn(s"[debug] readFiles setup in ${elapsed()}, repartitionDataFiles=$repartitionDataFiles, schema columns: ${readIndex.schema.fieldNames.length}")
      logger.warn(s"[debug] readFiles physical plan:")
      readIndex.queryExecution.executedPlan.toString().split("\n").foreach(line =>
        logger.warn(s"[debug]   $line"))
    }

    readIndex
  }

  /** Applies temporal deduplication to keep only the latest version of each
    * value for temporal index columns being used in the current join.
    *
    * Uses row_number() window function partitioned by the value column and
    * ordered by the timestamp column descending, keeping only rank 1.
    *
    * @param df The DataFrame read from data files
    * @param joinColumns The columns being used for the join
    * @return DataFrame with stale duplicates removed, or original if no temporal indexes apply
    */
  protected def applyTemporalDeduplication(
      df: DataFrame,
      joinColumns: Seq[String]
  ): DataFrame = {
    val temporalConfigs = metadata.temporal_indexes.asScala.toSeq
    val applicableConfigs = temporalConfigs.filter(tc => joinColumns.contains(tc.column))

    if (applicableConfigs.isEmpty) return df

    logger.warn(s"Applying temporal deduplication for columns: ${applicableConfigs.map(_.column).mkString(", ")}")
    applicableConfigs.foldLeft(df) { (accumDf, config) =>
      val w = Window
        .partitionBy(config.column)
        .orderBy(col(config.timestamp_column).desc_nulls_last)
      accumDf
        .withColumn("_ariadne_temporal_rank", row_number().over(w))
        .filter(col("_ariadne_temporal_rank") === 1)
        .drop("_ariadne_temporal_rank")
    }
  }

  /** Joins a DataFrame with indexed data files.
    *
    * This is the primary public API for index-based joins. The index locates
    * which files contain matching values, reads only those files, applies
    * temporal deduplication if applicable, then performs the join.
    *
    * @param df The DataFrame to join against indexed data
    * @param usingColumns The column names to join on (must be indexed)
    * @param joinType The join type: "inner", "left", "right", "left_semi", "left_anti", etc. (default: "inner")
    * @return The joined DataFrame
    * @throws ColumnNotFoundException if join columns are not in the schema or indexes
    */
  def join(
      df: DataFrame,
      usingColumns: Seq[String],
      joinType: String = "inner"
  ): DataFrame = {
    logger.warn(s"Index.join: $joinType join on columns ${usingColumns.mkString(", ")}")
    val outerJoinStart = System.currentTimeMillis()
    val indexDf = joinDf(df, usingColumns)
    if (debugEnabled) {
      logger.warn(s"[debug] joinDf returned in ${System.currentTimeMillis() - outerJoinStart}ms, now performing $joinType join")
    }
    val result = indexDf.join(df, usingColumns, joinType)
    logger.warn(s"Index.join: $joinType join setup completed in ${System.currentTimeMillis() - outerJoinStart}ms")
    if (debugEnabled) {
      logger.warn(s"[debug] Index.join complete in ${System.currentTimeMillis() - outerJoinStart}ms")
      logger.warn(s"[debug] result physical plan:")
      result.queryExecution.executedPlan.toString().split("\n").foreach(line =>
        logger.warn(s"[debug]   $line"))
    }
    result
  }
}
