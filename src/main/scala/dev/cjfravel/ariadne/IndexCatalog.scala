package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

/** Summary information for a single Ariadne index.
  *
  * Captures the index name, data format, all configured index types, tracked
  * file count, and cumulative indexed file size. Produced by
  * [[IndexCatalog.describe]] and [[IndexCatalog.describeAll]].
  *
  * @param name                  unique index name
  * @param format                data file format (e.g., "parquet", "csv", "json")
  * @param columns               union of all indexed column names across every index type
  * @param regularIndexes        columns with regular (array-of-values) indexes
  * @param bloomIndexes          columns with bloom filter indexes
  * @param computedIndexes       computed index aliases (derived via SQL expressions)
  * @param temporalIndexes       columns with temporal (version-dedup) indexes
  * @param rangeIndexes          columns with min/max range indexes
  * @param explodedFieldIndexes  aliases for exploded array field indexes
  * @param fileCount             number of files tracked by this index's file list
  * @param totalIndexedFileSize  cumulative size in bytes of all indexed files, or -1 if unknown
  */
case class IndexSummary(
    name: String,
    format: String,
    columns: Set[String],
    regularIndexes: Set[String],
    bloomIndexes: Set[String],
    computedIndexes: Set[String],
    temporalIndexes: Set[String],
    rangeIndexes: Set[String],
    explodedFieldIndexes: Set[String],
    fileCount: Long,
    totalIndexedFileSize: Long
)

/** Catalog for discovering, inspecting, and managing all Ariadne indexes
  * under the configured `spark.ariadne.storagePath`.
  *
  * `IndexCatalog` is stateless — every call scans the filesystem for the
  * current set of indexes. It complements the per-index API on [[Index]] by
  * providing a global view of the storage path.
  *
  * '''Usage:'''
  * {{{
  * import dev.cjfravel.ariadne.IndexCatalog
  *
  * // List all index names
  * val names: Seq[String] = IndexCatalog.list()
  *
  * // Get a summary of every index
  * val summaries: Seq[IndexSummary] = IndexCatalog.describeAll()
  *
  * // View all indexes as a DataFrame
  * IndexCatalog.toDF().show()
  *
  * // Fetch a specific Index instance
  * val index: Index = IndexCatalog.get("myIndex")
  * }}}
  */
object IndexCatalog {

  private val logger = LogManager.getLogger("ariadne")

  /** Lists the names of all indexes in the configured storage path.
    *
    * Scans the `{storagePath}/indexes/` directory for subdirectories that
    * contain a `metadata.json` file — this distinguishes real indexes from
    * stale or partially deleted directories.
    *
    * @param spark implicit SparkSession providing configuration
    * @return alphabetically sorted sequence of index names, or empty if none exist
    */
  def list()(implicit spark: SparkSession): Seq[String] = {
    val sparkSession = spark
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    val indexesDir = IndexPathUtils.storagePath
    if (!contextUser.exists(indexesDir)) {
      logger.warn("IndexCatalog: indexes directory does not exist, returning empty list")
      return Seq.empty
    }

    val statuses = contextUser.fs.listStatus(indexesDir)
    statuses
      .filter(_.isDirectory)
      .filter { status =>
        val metadataPath = new Path(status.getPath, "metadata.json")
        contextUser.fs.exists(metadataPath)
      }
      .map(_.getPath.getName)
      .sorted
      .toSeq
  }

  /** Checks whether an index with the given name exists.
    *
    * An index is considered to exist if its `metadata.json` file is present
    * in the indexes storage directory, or if its file list entry exists.
    *
    * @param name  the index name to check
    * @param spark implicit SparkSession
    * @return true if the index exists
    */
  def exists(name: String)(implicit spark: SparkSession): Boolean = {
    val sparkSession = spark
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    val metadataPath = new Path(new Path(IndexPathUtils.storagePath, name), "metadata.json")
    contextUser.fs.exists(metadataPath) || IndexPathUtils.exists(name)
  }

  /** Returns a summary of a single index.
    *
    * Reads the index's `metadata.json` and file list to populate an
    * [[IndexSummary]] with all configured index types and tracked file count.
    *
    * @param name  the index name
    * @param spark implicit SparkSession
    * @return the index summary
    * @throws IndexNotFoundException if the index does not exist
    */
  def describe(name: String)(implicit spark: SparkSession): IndexSummary = {
    if (!exists(name)) {
      throw new IndexNotFoundException(name)
    }
    buildSummary(name)
  }

  /** Returns summaries for all indexes in the storage path.
    *
    * Equivalent to calling [[describe]] for each name returned by [[list]],
    * but logs a single message for the batch operation. Indexes whose metadata
    * cannot be read are skipped with a warning.
    *
    * @param spark implicit SparkSession
    * @return sequence of [[IndexSummary]], one per valid index
    */
  def describeAll()(implicit spark: SparkSession): Seq[IndexSummary] = {
    val names = list()
    logger.warn(s"IndexCatalog: describing ${names.size} index(es)")
    names.flatMap { name =>
      try {
        Some(buildSummary(name))
      } catch {
        case e: Exception =>
          logger.warn(s"IndexCatalog: skipping index '$name' — ${e.getMessage}")
          None
      }
    }
  }

  /** Returns the names of all indexes that track a given file.
    *
    * Scans every index's file list to check whether `fileName` is present.
    * This is useful for determining which indexes need a `deleteFiles` call
    * when a source file has been removed from storage.
    *
    * {{{
    * val affected: Seq[String] = IndexCatalog.findIndexes("s3a://bucket/data/file1.parquet")
    * affected.foreach { name =>
    *   IndexCatalog.get(name).deleteFiles("s3a://bucket/data/file1.parquet")
    * }
    * }}}
    *
    * @param fileName the file path to search for across all indexes
    * @param spark    implicit SparkSession
    * @return alphabetically sorted names of indexes that track the file,
    *         or empty if the file is not found in any index
    */
  def findIndexes(fileName: String)(implicit spark: SparkSession): Seq[String] = {
    val names = list()
    logger.warn(s"IndexCatalog: searching ${names.size} index(es) for file '$fileName'")
    names.filter { name =>
      val fileListName = IndexPathUtils.fileListName(name)
      FileList.exists(fileListName) && FileList(fileListName).hasFile(fileName)
    }
  }

  /** Fetches an [[Index]] instance by reconnecting to an existing index.
    *
    * This is equivalent to calling `Index(name)` directly; it is provided
    * here for API completeness so callers can discover and fetch indexes
    * without importing [[Index]].
    *
    * @param name  the index name
    * @param spark implicit SparkSession
    * @return a fully initialized Index instance
    * @throws IndexNotFoundException if the index does not exist
    */
  def get(name: String)(implicit spark: SparkSession): Index = {
    if (!exists(name)) {
      throw new IndexNotFoundException(name)
    }
    Index(name)
  }

  /** Returns a DataFrame summarizing all indexes.
    *
    * The returned DataFrame has one row per index with the following columns:
    * {{{
    * name: String
    * format: String
    * regular_indexes: String   (comma-separated)
    * bloom_indexes: String     (comma-separated)
    * computed_indexes: String   (comma-separated)
    * temporal_indexes: String   (comma-separated)
    * range_indexes: String      (comma-separated)
    * exploded_field_indexes: String (comma-separated)
    * file_count: Long
    * total_indexed_file_size: Long
    * }}}
    *
    * @param spark implicit SparkSession
    * @return DataFrame with one row per index, or an empty DataFrame if no indexes exist
    */
  def toDF()(implicit spark: SparkSession): DataFrame = {
    val summaries = describeAll()
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("format", StringType, nullable = false),
      StructField("regular_indexes", StringType, nullable = false),
      StructField("bloom_indexes", StringType, nullable = false),
      StructField("computed_indexes", StringType, nullable = false),
      StructField("temporal_indexes", StringType, nullable = false),
      StructField("range_indexes", StringType, nullable = false),
      StructField("exploded_field_indexes", StringType, nullable = false),
      StructField("file_count", LongType, nullable = false),
      StructField("total_indexed_file_size", LongType, nullable = false)
    ))

    if (summaries.isEmpty) {
      return spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        schema
      )
    }

    val rows = summaries.map { s =>
      Row(
        s.name,
        s.format,
        s.regularIndexes.toSeq.sorted.mkString(", "),
        s.bloomIndexes.toSeq.sorted.mkString(", "),
        s.computedIndexes.toSeq.sorted.mkString(", "),
        s.temporalIndexes.toSeq.sorted.mkString(", "),
        s.rangeIndexes.toSeq.sorted.mkString(", "),
        s.explodedFieldIndexes.toSeq.sorted.mkString(", "),
        s.fileCount,
        s.totalIndexedFileSize
      )
    }
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )
  }

  /** Removes an index by name.
    *
    * Deletes the index storage directory (containing metadata, index tables,
    * staging, and large indexes) and the associated file list Delta table.
    *
    * @param name  the index name
    * @param spark implicit SparkSession
    * @return true if any resources were removed
    * @throws IndexNotFoundException if the index does not exist
    */
  def remove(name: String)(implicit spark: SparkSession): Boolean = {
    if (!exists(name)) {
      throw new IndexNotFoundException(name)
    }
    val sparkSession = spark
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }

    logger.warn(s"IndexCatalog: removing index '$name'")
    val indexDir = new Path(IndexPathUtils.storagePath, name)
    val indexDeleted = if (contextUser.fs.exists(indexDir)) {
      contextUser.delete(indexDir)
    } else false

    val fileListName = IndexPathUtils.fileListName(name)
    val fileListDeleted = if (FileList.exists(fileListName)) {
      FileList.remove(fileListName)
    } else false

    indexDeleted || fileListDeleted
  }

  // ---- internal helpers ----

  /** Builds an [[IndexSummary]] by reading metadata and file list for the
    * given index name.
    */
  private def buildSummary(
      name: String
  )(implicit spark: SparkSession): IndexSummary = {
    val index = Index(name)
    val md = index.metadata

    val regularIndexes = md.indexes.asScala.toSet
    val bloomIndexes = md.bloom_indexes.asScala.map(_.column).toSet
    val computedIndexes = md.computed_indexes.keySet().asScala.toSet
    val temporalIndexes = md.temporal_indexes.asScala.map(_.column).toSet
    val rangeIndexes = md.range_indexes.asScala.map(_.column).toSet
    val explodedFieldIndexes = md.exploded_field_indexes.asScala.map(_.as_column).toSet

    val allColumns = regularIndexes ++ bloomIndexes ++ computedIndexes ++
      temporalIndexes ++ rangeIndexes ++ explodedFieldIndexes

    val fileListName = IndexPathUtils.fileListName(name)
    val fileCount = if (FileList.exists(fileListName)) {
      FileList(fileListName).files.count()
    } else {
      0L
    }

    val totalSize = Option(md.total_indexed_file_size)
      .map(_.toLong)
      .getOrElse(-1L)

    IndexSummary(
      name = name,
      format = md.format,
      columns = allColumns,
      regularIndexes = regularIndexes,
      bloomIndexes = bloomIndexes,
      computedIndexes = computedIndexes,
      temporalIndexes = temporalIndexes,
      rangeIndexes = rangeIndexes,
      explodedFieldIndexes = explodedFieldIndexes,
      fileCount = fileCount,
      totalIndexedFileSize = totalSize
    )
  }
}
