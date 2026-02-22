package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

/** Trait providing join operations for Index instances.
  */
trait IndexJoinOperations extends IndexBuildOperations {
  self: Index =>

  /** Maps join column names to their corresponding storage column names.
    *
    * @param joinColumns
    *   The column names used in joins
    * @return
    *   Map from join column to storage column
    */
  protected def mapJoinColumnsToStorage(
      joinColumns: Seq[String]
  ): Map[String, String] = {
    val bloomColumnSet = bloomColumns

    joinColumns.map { joinCol =>
      // Check if this is a bloom filter column
      if (bloomColumnSet.contains(joinCol)) {
        joinCol -> (bloomColumnPrefix + joinCol)
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

    // Include both regular storage columns and bloom storage columns
    val allStorageColumns = this.storageColumns ++ this.bloomStorageColumns
    val storageColumnsToUse =
      columnMappings.values.toSet.intersect(allStorageColumns)
    logger.warn(s"Found indexes for ${storageColumnsToUse.mkString(",")}")

    // Get values from the user DataFrame using join column names
    val joinColumnsToUse = usingColumns.filter(col =>
      columnMappings.contains(col) && storageColumnsToUse.contains(
        columnMappings(col)
      )
    )

    // Get distinct values as a DataFrame (no collect to driver)
    val filteredValuesDf = df.select(joinColumnsToUse.map(col): _*).distinct()

    // Use the new DataFrame-based method to locate files
    val files = locateFilesFromDataFrame(
      filteredValuesDf,
      columnMappings,
      joinColumnsToUse
    )
    logger.warn(s"Found ${files.size} files in index")
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
    val readIndex = if (repartitionDataFiles) {
      maybeRepartition(readFiles(files))
    } else {
      readFiles(files)
    }
    if (debugEnabled) {
      logger.warn(s"[debug] readFiles setup in ${elapsed()}, repartitionDataFiles=$repartitionDataFiles, schema columns: ${readIndex.schema.fieldNames.length}")
      logger.warn(s"[debug] readFiles physical plan:")
      readIndex.queryExecution.executedPlan.toString().split("\n").foreach(line =>
        logger.warn(s"[debug]   $line"))
    }

    readIndex
  }

  /** Joins a DataFrame with the index.
    * @param df
    *   The DataFrame to join.
    * @param usingColumns
    *   The columns to use for the join.
    * @param joinType
    *   The type of join (default is "inner").
    * @return
    *   The resulting joined DataFrame.
    */
  def join(
      df: DataFrame,
      usingColumns: Seq[String],
      joinType: String = "inner"
  ): DataFrame = {
    val outerJoinStart = System.currentTimeMillis()
    val indexDf = joinDf(df, usingColumns)
    if (debugEnabled) {
      logger.warn(s"[debug] joinDf returned in ${System.currentTimeMillis() - outerJoinStart}ms, now performing $joinType join")
    }
    val result = indexDf.join(df, usingColumns, joinType)
    if (debugEnabled) {
      logger.warn(s"[debug] Index.join complete in ${System.currentTimeMillis() - outerJoinStart}ms")
      logger.warn(s"[debug] result physical plan:")
      result.queryExecution.executedPlan.toString().split("\n").foreach(line =>
        logger.warn(s"[debug]   $line"))
    }
    result
  }
}
