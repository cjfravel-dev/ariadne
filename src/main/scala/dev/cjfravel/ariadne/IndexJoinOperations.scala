package dev.cjfravel.ariadne

import java.util.Locale

import scala.collection.JavaConverters._

import dev.cjfravel.ariadne.exceptions.{ColumnNotFoundException, UnsupportedJoinTypeException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Trait providing join operations between DataFrames and indexed data.
 *
 * Orchestrates the join workflow:
 *   1. Maps join columns to their storage column names (regular, bloom, range, exploded) 2. Locates relevant files
 *      using the index (via [[IndexQueryOperations.locateFilesFromDataFrame]]) 3. Reads only the located data files
 *      (file-level pruning) 4. Applies temporal deduplication if applicable (keeps latest version per value) 5.
 *      Performs the final Spark DataFrame join
 *
 * An index join returns the '''minimal complete dataset''' for the join: an indexed value is never missed, and
 * index-side rows absent from the joined DataFrame are deliberately discarded. Join types whose result is defined by
 * those unmatched index-side rows are rejected — see [[IndexJoinOperations.validateJoinType]].
 *
 * Mixed into [[Index]] via `self: Index =>`.
 */
trait IndexJoinOperations extends IndexBuildOperations {
  self: Index =>

  /**
   * Maps join column names to their corresponding storage column names.
   *
   * Bloom filter columns are prefixed with the bloom prefix, range columns are prefixed with "range_", exploded field
   * columns are mapped to their backing array column, and all other columns map to themselves.
   *
   * @param joinColumns
   *   The column names used in joins
   * @return
   *   Map from each join column name to its storage column name in the index
   */
  protected def mapJoinColumnsToStorage(joinColumns: Seq[String]): Map[String, String] = {
    val bloomColumnSet = bloomColumns
    val rangeColumnSet = metadata.range_indexes.asScala.map(_.column).toSet

    joinColumns.map { joinCol =>
      if (bloomColumnSet.contains(joinCol)) {
        joinCol -> (bloomColumnPrefix + joinCol)
      } else if (rangeColumnSet.contains(joinCol)) {
        joinCol -> s"range_$joinCol"
      } else {
        // Check if this is an exploded field column
        val explodedMapping =
          metadata.exploded_field_indexes.asScala.find(_.as_column == joinCol)
        explodedMapping match {
          case Some(mapping) => joinCol -> mapping.as_column
          case None => joinCol -> joinCol
        }
      }
    }.toMap
  }

  /**
   * Locates and reads indexed data files relevant to the given DataFrame.
   *
   * Uses the index to identify which files contain matching values, then reads those files into a lazy DataFrame. If no
   * matching files are found, returns an empty DataFrame produced through the same read path as the populated branch,
   * so computed indexes, exploded fields and any active `select()` are applied and the schema matches either way. The
   * actual row-level filtering happens in the subsequent join in [[join]].
   *
   * Logs data pruning metrics (file count, data size saved) when available, and includes detailed file-level debug
   * information when debug mode is enabled.
   *
   * @param df
   *   The DataFrame to match against the index.
   * @param usingColumns
   *   The columns used for the join.
   * @return
   *   A lazy DataFrame containing data from indexed files, or an empty DataFrame if no files match.
   * @throws dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
   *   if join columns are not in the selected columns, schema, or indexes
   * @throws IllegalArgumentException
   *   if none of the join columns have indexes, or if df/usingColumns are null/empty
   */
  protected def joinDf(df: DataFrame, usingColumns: Seq[String]): DataFrame = {
    require(df != null, "DataFrame must not be null")
    require(usingColumns != null && usingColumns.nonEmpty, "usingColumns must not be null or empty")
    val joinStart = System.currentTimeMillis()
    def elapsed(): String = {
      val ms = System.currentTimeMillis() - joinStart
      if (ms > 60000) f"${ms / 60000}m ${ms % 60000 / 1000}s"
      else if (ms > 1000) f"${ms / 1000.0}%.1fs"
      else s"${ms}ms"
    }

    if (debugEnabled) {
      logger.warn(s"[debug] joinDf started: index=$name, usingColumns=${usingColumns.mkString(",")}")
      logger.warn(s"[debug] input df schema: ${df.schema.fieldNames.mkString(",")}")
    }

    // Validate that join columns are included in selected columns (if selection is active)
    // or exist in the schema (if no selection)
    getSelectedColumns match {
      case Some(selectedCols) =>
        val missingJoinCols = usingColumns.filterNot(selectedCols.contains)
        if (missingJoinCols.nonEmpty) {
          throw new ColumnNotFoundException(
            s"Join columns must be included in selected columns. Missing: ${missingJoinCols.mkString(", ")}")
        }
      case None =>
        // No selection active, but still validate columns exist in schema or are available indexes
        val invalidJoinCols =
          usingColumns.filterNot { colName =>
            SchemaHelper.fieldExists(storedSchema, colName) || this.indexes
              .contains(colName)
          }
        if (invalidJoinCols.nonEmpty) {
          throw new ColumnNotFoundException(
            s"Join columns not found in schema or indexes: ${invalidJoinCols.mkString(", ")}")
        }
    }

    // Map join columns to storage columns
    val columnMappings = mapJoinColumnsToStorage(usingColumns)

    // Include both regular storage columns and bloom/range storage columns
    val allStorageColumns =
      this.storageColumns ++ this.bloomStorageColumns ++ this.rangeStorageColumns
    val storageColumnsToUse =
      columnMappings.values.toSet.intersect(allStorageColumns)
    logger.warn(s"Found indexes for ${storageColumnsToUse.mkString(",")}")

    // Get values from the user DataFrame using join column names
    val joinColumnsToUse =
      usingColumns.filter(col => columnMappings.contains(col) && storageColumnsToUse.contains(columnMappings(col)))

    if (joinColumnsToUse.isEmpty) {
      val unindexed = usingColumns.filterNot(c => storageColumnsToUse.contains(columnMappings.getOrElse(c, c)))
      throw new IllegalArgumentException(
        s"None of the join columns [${usingColumns.mkString(", ")}] have indexes. " +
          s"Unindexed columns: [${unindexed.mkString(", ")}]. " +
          "Add indexes on these columns before joining.")
    }

    // Get values from the user DataFrame using join column names
    val filteredValuesDf = df.select(joinColumnsToUse.map(col): _*)

    // Use the new DataFrame-based method to locate files
    val files = locateFilesFromDataFrame(filteredValuesDf, columnMappings, joinColumnsToUse)
    logger.warn(s"Found ${files.size} files in index")

    if (files.isEmpty) {
      logger.warn(s"No matching files found in index '$name', returning empty DataFrame")
      // Build the empty result through the same read path the populated branch uses, rather than
      // from `storedSchema` verbatim. `createBaseDataFrame` short-circuits on an empty file set,
      // so no IO happens, but computed indexes, exploded fields and the active `select()` are all
      // applied. Deriving both branches from one path keeps the result schema independent of
      // whether any file matched, and stops the two from drifting apart again.
      readFiles(files)
    } else {

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
              val matchedSizeRows =
                indexDf
                  .join(filesDf, Seq("filename"), "inner")
                  .agg(sum("file_size"))
                  .take(1)
              if (matchedSizeRows.nonEmpty) {
                val matchedSizeResult = matchedSizeRows(0)
                val matchedSize =
                  if (matchedSizeResult.isNullAt(0)) 0L
                  else matchedSizeResult.getLong(0)
                val totalGB = totalIndexedSize / (1024.0 * 1024.0 * 1024.0)
                val matchedGB = matchedSize / (1024.0 * 1024.0 * 1024.0)
                val savedPercent =
                  if (totalIndexedSize > 0)
                    ((totalIndexedSize - matchedSize) * 100.0 / totalIndexedSize).toInt
                  else 0
                logger.warn(
                  f"Index pruning: loaded ${files.size}%d of $totalFiles%d files " +
                    f"($matchedGB%.2f GB of $totalGB%.2f GB) — $savedPercent%%  data pruned")
              }
            }
          }
        }
      } catch {
        case e: Exception =>
          logger.warn(
            s"Failed to compute pruning metrics for index '$name': ${e.getClass.getSimpleName}: ${e.getMessage}")
      }

      if (debugEnabled) {
        logger.warn(s"[debug] locateFiles completed in ${elapsed()}, files: ${files.size}")
        if (files.nonEmpty) {
          try {
            val fileSizes =
              files.toSeq
                .map { f =>
                  val path = new org.apache.hadoop.fs.Path(f)
                  val fileFs =
                    path.getFileSystem(spark.sparkContext.hadoopConfiguration)
                  val size = fileFs.getFileStatus(path).getLen
                  (f, size)
                }
                .sortBy(-_._2)
            val totalBytes = fileSizes.map(_._2).sum
            val totalMB = totalBytes / (1024.0 * 1024.0)
            val totalGB = totalBytes / (1024.0 * 1024.0 * 1024.0)
            logger.warn(f"[debug] total file size: $totalMB%.1fMB ($totalGB%.2fGB) across ${files.size} files")
            val avgMB = totalMB / files.size
            if (fileSizes.nonEmpty) {
              val maxFile = fileSizes.head
              val minFile = fileSizes.last
              val maxMB = maxFile._2 / (1024.0 * 1024.0)
              val minMB = minFile._2 / (1024.0 * 1024.0)
              logger.warn(f"[debug] file sizes: avg=$avgMB%.1fMB, max=$maxMB%.1fMB, min=$minMB%.1fMB")
              logger.warn(f"[debug] largest file: $maxMB%.1fMB -> ${maxFile._1}")
              logger.warn(f"[debug] smallest file: $minMB%.1fMB -> ${minFile._1}")
            }
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
      }

      // Read the data files located by the index.
      // repartitionDataFiles controls whether the data files are repartitioned.
      // Default is false — data files keep their natural parquet partitioning.
      // Enable when reading all columns from very large indexes to reduce
      // per-executor memory pressure.
      logger.warn(s"Reading ${files.size} data files from index '$name'")
      // Temporal deduplication orders by each applicable temporal index's timestamp column.
      // If select() pruned that column away, read it anyway and drop it after deduplication
      // so the caller's projection is preserved. Deduplication resolves the configured
      // timestamp path against the DataFrame, so a nested path such as `meta.updatedAt`
      // requires its root struct (`meta`) to be present rather than the flattened leaf that
      // projecting the dotted path would produce.
      val temporalTimestampRootColumns =
        getSelectedColumns match {
          case Some(selectedCols) =>
            metadata.temporal_indexes.asScala.toSeq
              .filter(tc => usingColumns.contains(tc.column))
              .map(tc => tc.timestamp_column.split("\\.").head)
              .filterNot(selectedCols.contains)
              .distinct
          case None => Seq.empty
        }
      if (temporalTimestampRootColumns.nonEmpty) {
        logger.warn(
          s"Reading unselected temporal timestamp column(s) for deduplication: " +
            s"${temporalTimestampRootColumns.mkString(", ")}")
      }
      val rawReadIndex =
        if (repartitionDataFiles) {
          maybeRepartition(readFiles(files, temporalTimestampRootColumns))
        } else {
          readFiles(files, temporalTimestampRootColumns)
        }
      // Apply temporal deduplication if any temporal indexes are being used in this join
      val readIndex =
        applyTemporalDeduplication(rawReadIndex, usingColumns)
          .drop(temporalTimestampRootColumns: _*)
      if (debugEnabled) {
        logger.warn(
          s"[debug] readFiles setup in ${elapsed()}, repartitionDataFiles=$repartitionDataFiles, " +
            s"schema columns: ${readIndex.schema.fieldNames.length}")
        logger.warn(s"[debug] readFiles physical plan:")
        readIndex.queryExecution.executedPlan
          .toString()
          .split("\n")
          .foreach(line => logger.warn(s"[debug]   $line"))
      }

      logger.warn(s"joinDf completed for index '$name' in ${elapsed()}")
      readIndex
    }
  }

  /**
   * Applies temporal deduplication to keep only the latest version of each value for temporal index columns being used
   * in the current join.
   *
   * Deduplication runs in two stages per applicable temporal index:
   *
   *   1. '''Stale rejection.''' Rows older than the latest timestamp the ''index'' records for their value are dropped.
   *      The index covers every file, so this stage is not limited to the rows that survived file pruning.
   *   1. '''Tie collapse.''' A `row_number()` window partitioned by the value column, ordered by the timestamp column
   *      descending, keeps one row per value. All ranks are computed before filtering to rows ranked first for every
   *      applicable temporal index.
   *
   * The first stage exists because file pruning intersects across index types. A query that constrains a temporal
   * column and some other indexed column reads only files satisfying ''both'', which can exclude the file holding a
   * value's newest version. Ranking within only those rows would then promote a superseded row to "latest" and emit it
   * as though it were current — the one result an index-backed join must never produce. Comparing against the index
   * instead drops that row, which is correct: a value whose current version does not satisfy the query has no place in
   * the result.
   *
   * The comparison is `>=` rather than `=` deliberately. The recorded maximum is a ''lower bound'' on the true latest
   * timestamp — it can lag when rows are still in staging, or when a large index was pruned — and a lower bound is all
   * this stage needs. Any row at or beyond it survives to stage 2, so a genuinely newest row is never dropped even when
   * the index is incomplete; only rows provably older than something already indexed are removed. A value the index
   * does not know at all yields a null bound and is left to stage 2 alone.
   *
   * @note
   *   Stage 1 adds one scan of the temporal index and one join per applicable temporal column. This is the cost of
   *   ranking against the whole dataset rather than against whatever pruning happened to leave behind.
   *
   * @param df
   *   The DataFrame read from data files
   * @param joinColumns
   *   The columns being used for the join
   * @return
   *   DataFrame with stale duplicates removed, or original if no temporal indexes apply
   */
  private[ariadne] def applyTemporalDeduplication(df: DataFrame, joinColumns: Seq[String]): DataFrame = {
    val temporalConfigs = metadata.temporal_indexes.asScala.toSeq
    val applicableConfigs =
      temporalConfigs.filter(tc => joinColumns.contains(tc.column))

    if (applicableConfigs.isEmpty) {
      df
    } else {
      logger.warn(s"Applying temporal deduplication for columns: ${applicableConfigs.map(_.column).mkString(", ")}")

      val currentDf = applicableConfigs.foldLeft(df)((accumDf, config) => rejectSupersededRows(accumDf, config))

      val (rankedDf, _, rankColumns) =
        applicableConfigs.zipWithIndex.foldLeft((currentDf, currentDf.columns.toSet, Vector.empty[String])) {
          case ((accumDf, usedColumns, accumulatedRankColumns), (config, index)) =>
            val rankColumn = unusedWorkingColumn(s"_ariadne_temporal_rank_$index", usedColumns)
            val w =
              Window
                .partitionBy(config.column)
                .orderBy(col(config.timestamp_column).desc_nulls_last)
            (
              accumDf.withColumn(rankColumn, row_number().over(w)),
              usedColumns + rankColumn,
              accumulatedRankColumns :+ rankColumn)
        }
      val allLatest = rankColumns.map(rankColumn => col(rankColumn) === 1).reduce(_ && _)
      val result = rankedDf.filter(allLatest).drop(rankColumns: _*)
      logger.debug(s"Temporal deduplication applied for ${applicableConfigs.size} column(s)")
      result
    }
  }

  /**
   * Drops rows whose timestamp is older than the latest one the index records for their value.
   *
   * Implements stage 1 of [[applyTemporalDeduplication]]. The bound comes from the temporal index rather than from the
   * rows in hand, so a row is judged against every version of its value in the dataset, not only the versions that
   * survived file pruning. See [[applyTemporalDeduplication]] for why the comparison is `>=`.
   *
   * Returns `df` unchanged when the index table does not exist yet, since there is then no bound to judge against.
   *
   * @param df
   *   the rows read from data files
   * @param config
   *   the temporal index whose value and timestamp columns define the comparison
   * @return
   *   `df` without rows the index proves are superseded
   */
  private def rejectSupersededRows(df: DataFrame, config: TemporalIndexConfig): DataFrame =
    index match {
      case None =>
        logger.warn(s"Index table not found for index '$name'; skipping stale-version rejection for '${config.column}'")
        df
      case Some(indexDf) =>
        val used = df.columns.toSet
        val valueColumn = unusedWorkingColumn("_ariadne_indexed_value", used)
        val maxTsColumn = unusedWorkingColumn("_ariadne_indexed_max_ts", used + valueColumn)

        val latestPerValue =
          loadTemporalColumnIndex(indexDf, config.column)
            .groupBy(col("_value"))
            .agg(max(col("_max_ts")).alias(maxTsColumn))
            .withColumnRenamed("_value", valueColumn)

        df.join(latestPerValue, df(config.column) <=> latestPerValue(valueColumn), "left")
          .where(col(maxTsColumn).isNull || col(config.timestamp_column) >= col(maxTsColumn))
          .drop(valueColumn, maxTsColumn)
    }

  /**
   * Derives a transient column name that does not collide with names already in use.
   *
   * Any user column name is legal, so a fixed literal such as `_ariadne_temporal_rank_0` can collide with a genuine
   * column and make the subsequent reference ambiguous. A numeric suffix is appended until the name is free. These
   * names never leave the method that allocates them.
   *
   * @param base
   *   the preferred name
   * @param usedColumns
   *   names that must be avoided
   * @return
   *   a name not present in `usedColumns`
   * @throws IllegalStateException
   *   if no free name can be derived
   */
  private def unusedWorkingColumn(base: String, usedColumns: Set[String]): String =
    Iterator
      .from(0)
      .map(index => if (index == 0) base else s"${base}_$index")
      .find(name => !usedColumns.contains(name))
      .fold(throw new IllegalStateException(s"Unable to allocate a temporary column named '$base'"))(identity)

  /**
   * Joins a DataFrame with indexed data files.
   *
   * This is the primary public API for index-based joins. The index locates which files contain matching values, reads
   * only those files, applies temporal deduplication if applicable, then performs the Spark DataFrame join.
   *
   * The join direction is `indexedData.join(df)` — the index-located data is on the left. For the reverse direction,
   * use [[Index.DataFrameOps.join]].
   *
   * '''What this returns:''' the minimal complete dataset for the join. Every indexed value that matches `df` is found,
   * and index-side rows absent from `df` are discarded. Because pruning selects whole files, unmatched index-side rows
   * that share a file with a match are still read, so the result can include incidental rows and can change after
   * [[Index.compact]]. Join types whose result is defined by those rows — `left`, `full` and `left_anti`, since the
   * index is on the left here — are rejected rather than returning a file-layout-dependent answer.
   *
   * @example
   *   {{{
   * val index = Index("orders", ordersSchema)
   * index.addIndex("customer_id")
   * index.update
   *
   * val lookupDf = Seq("c1", "c2").toDF("customer_id")
   * val result = index.join(lookupDf, Seq("customer_id"))
   * // Keep every row of lookupDf, matched or not:
   * val rightResult = index.join(lookupDf, Seq("customer_id"), "right")
   *   }}}
   *
   * @param df
   *   The DataFrame to join against indexed data
   * @param usingColumns
   *   The column names to join on (must be indexed columns)
   * @param joinType
   *   The Spark join type. Supported here: "inner", "cross", "left_semi", "right", "right_outer" (default: "inner"). To
   *   keep every row of the indexed dataset regardless of matches, read the data files directly.
   * @return
   *   The joined DataFrame
   * @throws dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
   *   if join columns are not in the schema or indexes
   * @throws dev.cjfravel.ariadne.exceptions.UnsupportedJoinTypeException
   *   if `joinType` is "left", "full" or "left_anti" (or an alias), which depend on unmatched index-side rows
   * @throws IllegalArgumentException
   *   if none of the join columns have indexes
   */
  def join(df: DataFrame, usingColumns: Seq[String], joinType: String = "inner"): DataFrame = {
    IndexJoinOperations.validateJoinType(joinType, "left")
    logger.warn(s"Index.join on index '$name': $joinType join on columns ${usingColumns.mkString(", ")}")
    try {
      val outerJoinStart = System.currentTimeMillis()
      val indexDf = joinDf(df, usingColumns)
      if (debugEnabled) {
        logger.warn(
          s"[debug] joinDf returned in ${System.currentTimeMillis() - outerJoinStart}ms, now performing $joinType join")
      }
      val result = indexDf.join(df, usingColumns, joinType)
      logger.warn(s"Index.join on index '$name': $joinType join setup completed in ${System
          .currentTimeMillis() - outerJoinStart}ms")
      if (debugEnabled) {
        logger.warn(s"[debug] Index.join complete in ${System.currentTimeMillis() - outerJoinStart}ms")
        logger.warn(s"[debug] result physical plan:")
        result.queryExecution.executedPlan
          .toString()
          .split("\n")
          .foreach(line => logger.warn(s"[debug]   $line"))
      }
      result
    } catch {
      case e: Exception =>
        logger.warn(s"Join failed for index '$name' with columns [${usingColumns
            .mkString(", ")}]: ${e.getMessage}")
        throw e
    }
  }
}

/**
 * Join-type rules for index-backed joins.
 *
 * An index locates whole files, not individual rows, and an index join returns the minimal complete dataset for that
 * join: matching values are never missed, and index-side rows absent from the joined DataFrame are discarded. The
 * unmatched index-side rows that do survive pruning are the ones that happened to share a file with a match, so they
 * are an artifact of file layout and change after compaction.
 *
 * Join types whose result is defined by those rows therefore have no stable meaning and are rejected up front rather
 * than returning a file-layout-dependent answer.
 */
object IndexJoinOperations {

  /** Join types that surface unmatched rows from the left operand. */
  private val LeftPreservingJoinTypes = Set("left", "leftouter", "leftanti", "anti", "full", "fullouter", "outer")

  /** Join types that surface unmatched rows from the right operand. */
  private val RightPreservingJoinTypes = Set("right", "rightouter", "full", "fullouter", "outer")

  /**
   * Normalizes a Spark join type for comparison by lowercasing and stripping underscores, so that `left_outer`,
   * `leftOuter` and `leftouter` all compare equal.
   *
   * @param joinType
   *   the join type as written by the caller
   * @return
   *   the normalized form
   */
  private def normalize(joinType: String): String =
    joinType.toLowerCase(Locale.ROOT).replace("_", "").trim

  /**
   * Rejects join types that cannot be answered meaningfully when the index-located data occupies `indexSide`.
   *
   * @param joinType
   *   the join type requested by the caller
   * @param indexSide
   *   the side the index-located data occupies, `"left"` or `"right"`
   * @throws dev.cjfravel.ariadne.exceptions.UnsupportedJoinTypeException
   *   if the join type's result would be defined by unmatched index-side rows
   * @throws IllegalArgumentException
   *   if `joinType` is null or blank
   */
  def validateJoinType(joinType: String, indexSide: String): Unit = {
    require(joinType != null && joinType.trim.nonEmpty, "joinType must not be null or empty")
    val normalized = normalize(joinType)
    val rejected =
      if (indexSide == "left") LeftPreservingJoinTypes else RightPreservingJoinTypes
    if (rejected.contains(normalized)) {
      throw UnsupportedJoinTypeException(joinType, indexSide, supportedFor(indexSide))
    }
  }

  /**
   * Lists the join types that are meaningful when the index-located data occupies `indexSide`.
   *
   * @param indexSide
   *   the side the index-located data occupies, `"left"` or `"right"`
   * @return
   *   the supported join types, for use in error messages and documentation
   */
  def supportedFor(indexSide: String): Seq[String] = {
    val dataFrameSideOuter =
      if (indexSide == "left") Seq("right", "right_outer") else Seq("left", "left_outer", "left_anti")
    Seq("inner", "cross", "left_semi") ++ dataFrameSideOuter
  }
}
