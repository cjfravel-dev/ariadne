package dev.cjfravel.ariadne.catalog

import dev.cjfravel.ariadne.{FileList, Index, IndexMetadata, IndexPathUtils}
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, V1Scan}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Builds a scan for reading Ariadne index source data with filter pushdown.
  *
  * Implements [[SupportsPushDownFilters]] to convert `WHERE` clause filters
  * on indexed columns into [[dev.cjfravel.ariadne.IndexQueryOperations.locateFiles]]
  * calls, pruning the source file list before reading.
  *
  * '''Thread safety:''' Instances are created per scan by [[AriadneTable.newScanBuilder]]
  * and are not shared across threads.
  *
  * @param indexName the Ariadne index name
  * @param sourceSchema the schema of the source data files
  * @param metadata the index metadata
  *
  * @see [[AriadneV1Scan]] for the scan implementation
  */
class AriadneScanBuilder(
    indexName: String,
    sourceSchema: StructType,
    metadata: IndexMetadata
) extends ScanBuilder
    with SupportsPushDownFilters {

  private val logger = LogManager.getLogger("ariadne")
  private var pushedFilterArray: Array[Filter] = Array.empty

  /** Pushes filters to the scan for file-level pruning via the index.
    *
    * Filters on indexed columns (`EqualTo`, `In`) are accepted and used
    * to call `locateFiles()` to prune the source file list. Filters on
    * non-indexed columns are returned for Spark to apply post-scan.
    *
    * @param filters the filters to push
    * @return filters that could NOT be pushed (must be applied post-scan)
    */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val indexedColumns = allIndexedColumns
    val pushable = filters.filter { f =>
      extractFilterColumn(f).exists(indexedColumns.contains)
    }
    pushedFilterArray = pushable
    logger.warn(
      s"AriadneScanBuilder: accepted ${pushable.length} filter(s) for file pruning " +
        s"on index '$indexName' (all ${filters.length} filters applied post-scan by Spark)"
    )
    // Return ALL filters — Spark must apply them post-scan since our
    // file-level pruning via locateFiles is coarse-grained (file, not row)
    filters
  }

  /** Returns the filters that were successfully pushed for file-level pruning.
    *
    * @return array of pushed filters
    */
  override def pushedFilters(): Array[Filter] = pushedFilterArray

  /** Builds a [[AriadneV1Scan]] with the current pushed filters.
    *
    * @return a new V1-based scan for reading Ariadne source data
    */
  override def build(): Scan = {
    new AriadneV1Scan(indexName, sourceSchema, metadata, pushedFilterArray)
  }

  private def allIndexedColumns: Set[String] = {
    val regular = metadata.indexes.asScala.toSet
    val bloom = metadata.bloom_indexes.asScala.map(_.column).toSet
    val computed = metadata.computed_indexes.asScala.keys.toSet
    val temporal = metadata.temporal_indexes.asScala.map(_.column).toSet
    val range = metadata.range_indexes.asScala.map(_.column).toSet
    val exploded = metadata.exploded_field_indexes.asScala.map(_.as_column).toSet
    regular ++ bloom ++ computed ++ temporal ++ range ++ exploded
  }

  private def extractFilterColumn(filter: Filter): Option[String] = filter match {
    case EqualTo(attr, _) => Some(attr)
    case In(attr, _)      => Some(attr)
    case _                => None
  }
}

/** V1-based scan that delegates file reading to Spark's built-in readers.
  *
  * Uses `V1Scan` to create a `BaseRelation` that reads source data files
  * using `spark.read` with the appropriate format (CSV, Parquet, JSON).
  * This avoids reimplementing file readers and gives full Spark optimization
  * (predicate pushdown to Parquet, column pruning, etc.).
  *
  * When WHERE filters were pushed through [[AriadneScanBuilder]], the file
  * list is pruned via [[dev.cjfravel.ariadne.IndexQueryOperations.locateFiles]]
  * before reading.
  *
  * '''Driver memory note:''' `locateMatchingFiles` collects the file list to
  * the driver. This is bounded by the number of indexed files (not row count),
  * but extremely large file lists could pressure driver memory.
  *
  * '''Thread safety:''' Instances are created per scan and are not shared
  * across threads.
  *
  * @param indexName the Ariadne index name
  * @param sourceSchema the schema of the source data files
  * @param metadata the index metadata
  * @param pushedFilters the filters pushed from the ScanBuilder
  */
class AriadneV1Scan(
    indexName: String,
    sourceSchema: StructType,
    metadata: IndexMetadata,
    pushedFilters: Array[Filter]
) extends V1Scan {

  private val logger = LogManager.getLogger("ariadne")

  /** Returns the schema of the source data files.
    *
    * @return the source data schema stored in index metadata
    */
  override def readSchema(): StructType = sourceSchema

  /** Creates a V1 BaseRelation that reads the source data files.
    *
    * If filters were pushed, uses `locateFiles()` to prune the file list.
    * Otherwise reads all indexed files.
    *
    * @param sqlCtx the SQLContext provided by Spark
    * @return a BaseRelation backed by Ariadne's source data files
    */
  override def toV1TableScan[T <: BaseRelation with TableScan](
      sqlCtx: SQLContext
  ): T = {
    implicit val spark: SparkSession = sqlCtx.sparkSession

    val files = locateMatchingFiles()
    logger.warn(
      s"AriadneV1Scan: reading ${files.size} file(s) for index '$indexName'" +
        s" (${pushedFilters.length} pushed filter(s))"
    )

    new AriadneBaseRelation(sqlCtx, indexName, sourceSchema, metadata, files)
      .asInstanceOf[T]
  }

  private def locateMatchingFiles()(implicit spark: SparkSession): Set[String] = {
    val fileListName = IndexPathUtils.fileListName(indexName)
    if (!FileList.exists(fileListName)) {
      logger.warn(s"AriadneV1Scan: no file list found for index '$indexName'")
      return Set.empty
    }

    val allFiles = FileList(fileListName).files
      .select("filename")
      .collect()
      .flatMap(r => Option(r.getString(0)))
      .toSet

    if (pushedFilters.isEmpty) {
      allFiles
    } else {
      val filterMap = buildLocateFilesMap()
      if (filterMap.isEmpty) {
        allFiles
      } else {
        val index = Index(indexName)
        val located = index.locateFiles(filterMap)
        logger.warn(
          s"AriadneV1Scan: locateFiles pruned ${allFiles.size} → ${located.size} file(s)"
        )
        located
      }
    }
  }

  private def buildLocateFilesMap(): Map[String, Array[Any]] = {
    pushedFilters.flatMap {
      case EqualTo(attr, value) => Some(attr -> Array[Any](value))
      case In(attr, values)     => Some(attr -> values.map(_.asInstanceOf[Any]))
      case _                    => None
    }.groupBy(_._1)
      .map { case (col, entries) =>
        col -> entries.flatMap(_._2).distinct
      }
  }
}

/** BaseRelation that reads Ariadne index source data files.
  *
  * Delegates to Spark's built-in format readers (CSV, Parquet, JSON) using
  * `spark.read`. Applies computed indexes and exploded field transformations
  * from the index metadata.
  *
  * Extends both `TableScan` and `PrunedFilteredScan`. Spark prefers
  * `PrunedFilteredScan.buildScan(requiredColumns, filters)` when available,
  * falling back to `TableScan.buildScan()` only if column pruning is not needed.
  *
  * '''Thread safety:''' Instances are created per scan and are not shared
  * across threads.
  *
  * @param sqlCtx the SQLContext
  * @param indexName the Ariadne index name
  * @param sourceSchema the source data schema
  * @param metadata the index metadata
  * @param files the set of source file paths to read
  */
class AriadneBaseRelation(
    override val sqlContext: SQLContext,
    indexName: String,
    sourceSchema: StructType,
    metadata: IndexMetadata,
    files: Set[String]
) extends BaseRelation
    with TableScan
    with PrunedFilteredScan {

  private val logger = LogManager.getLogger("ariadne")

  /** Returns the source data schema.
    *
    * @return the schema of the original source data files
    */
  override def schema: StructType = sourceSchema

  /** Full table scan — reads all source data files.
    *
    * Used when Spark doesn't push column pruning or filters.
    *
    * @return RDD of source data rows
    */
  override def buildScan(): RDD[Row] = {
    buildScan(sourceSchema.fieldNames, Array.empty)
  }

  /** Builds an RDD of Rows by reading source data files.
    *
    * Uses [[dev.cjfravel.ariadne.Index]] internally to read files with
    * computed indexes and exploded fields applied, then selects only
    * the required columns.
    *
    * @param requiredColumns columns Spark needs from this scan
    * @param filters additional filters Spark wants applied (best-effort)
    * @return RDD of source data rows
    */
  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter]
  ): RDD[Row] = {
    implicit val spark: SparkSession = sqlContext.sparkSession
    val startTime = System.currentTimeMillis()

    if (files.isEmpty) {
      logger.warn(s"AriadneBaseRelation: no files to read for index '$indexName'")
      return spark.sparkContext.emptyRDD[Row]
    }

    val index = Index(indexName)
    val rawDf = index.readFiles(files)

    // Apply temporal deduplication for all temporal-indexed columns.
    // Temporal indexes mean "only the latest version" — this applies to
    // all catalog reads (SELECT, WHERE, JOIN), not just joins.
    val temporalCols = metadata.temporal_indexes.asScala.map(_.column).toSeq
    val df = index.applyTemporalDeduplication(rawDf, temporalCols)

    val selected = if (requiredColumns.nonEmpty) {
      df.select(requiredColumns.map(org.apache.spark.sql.functions.col): _*)
    } else {
      // Even when no specific columns requested, limit to source schema
      // to exclude computed/exploded columns added by readFiles
      df.select(sourceSchema.fieldNames.map(org.apache.spark.sql.functions.col): _*)
    }

    logger.warn(
      s"AriadneBaseRelation: scan setup for index '$indexName' " +
        s"(${files.size} files, ${requiredColumns.length} columns) " +
        s"in ${System.currentTimeMillis() - startTime}ms"
    )
    selected.rdd
  }
}
