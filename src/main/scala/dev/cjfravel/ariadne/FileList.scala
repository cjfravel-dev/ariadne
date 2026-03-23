package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{Logger, LogManager}
import java.time.Instant
import java.sql.Timestamp

/** Manages a tracked list of files associated with an Ariadne index.
  *
  * Each file list is persisted as a Delta Lake table with two columns:
  *   - `filename` (`StringType`, not nullable) — the file path
  *   - `addedAt` (`TimestampType`, not nullable) — when the file was registered
  *
  * The DataFrame is lazily loaded and cached in memory after the first access.
  * Mutations (add, remove) invalidate the cache so the next read re-loads from
  * the Delta table.
  *
  * @param name
  *   the name of this file list, typically `"[ariadne_index] {indexName}"`
  * @param spark
  *   implicit SparkSession for Delta Lake operations
  */
case class FileList private (
    name: String
)(implicit val spark: SparkSession)
    extends AriadneContextUser {
  override val logger: Logger = LogManager.getLogger("ariadne")

  override lazy val storagePath: Path = new Path(FileList.storagePath, name)

  /** Cached DataFrame of tracked files. Set to `null` to force reload. */
  private var _files: DataFrame = _

  /** Loads or returns the cached file list DataFrame.
    *
    * On first access (or after cache invalidation), reads the Delta table at
    * `storagePath`. If the table does not yet exist, returns an empty DataFrame
    * with the expected schema. The loaded result is cached for subsequent
    * calls.
    *
    * @param spark
    *   the SparkSession to use for reading
    * @return
    *   DataFrame with columns (filename: String, addedAt: Timestamp)
    */
  private def files(spark: SparkSession): DataFrame = {
    if (_files == null) {
      _files = delta(storagePath) match {
        case Some(delta) =>
          val df = delta.toDF
          val count = df.count()
          logger.warn(
            s"Loaded file list '$name' from Delta table ($count files)"
          )
          df
        case None =>
          logger.warn(
            s"File list '$name' not yet persisted, using empty DataFrame"
          )
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[Row],
            StructType(
              Seq(
                StructField("filename", StringType, nullable = false),
                StructField("addedAt", TimestampType, nullable = false)
              )
            )
          )
      }
    } else {
      logger.debug(s"Returning cached file list '$name'")
    }

    _files
  }

  /** Returns the DataFrame of tracked files with filename and addedAt columns.
    *
    * Lazily loaded from the Delta table on first access; subsequent calls
    * return the cached DataFrame until the cache is invalidated by a mutation.
    *
    * @return
    *   DataFrame with columns (filename: String, addedAt: Timestamp)
    */
  def files: DataFrame = files(spark)

  private def addFile(spark: SparkSession, fileNames: String*): Unit = {
    import spark.implicits._
    val existing = files.select("filename").as[String].collect().toSet
    val toAdd = fileNames.toSet.diff(existing)
    if (toAdd.isEmpty) {
      logger.warn("All files were already added")
      return
    }

    val ts = Timestamp.from(Instant.now())
    val newFilesData = toAdd.toList.map(filename => Row(filename, ts))
    val newFiles = spark.createDataFrame(
      spark.sparkContext.parallelize(newFilesData),
      StructType(
        Seq(
          StructField("filename", StringType, nullable = false),
          StructField("addedAt", TimestampType, nullable = false)
        )
      )
    )

    val originalFiles = _files
    _files = _files.union(newFiles)
    try {
      write
    } catch {
      case e: Exception =>
        _files = originalFiles
        throw e
    }
    logger.warn(s"Added ${toAdd.size} files to FileList $name")
  }

  /** Registers new files in the file list.
    *
    * Files already present are silently skipped. The additions are written to
    * the Delta table immediately; if the write fails, the in-memory cache is
    * rolled back to its previous state.
    *
    * @param fileNames
    *   one or more file paths to add
    */
  def addFile(fileNames: String*): Unit = addFile(spark, fileNames: _*)

  /** Removes files from the file list using a Delta merge-delete.
    *
    * After deletion, the in-memory cache is invalidated so the next read
    * reflects the updated state.
    *
    * @param fileNames
    *   one or more file paths to remove
    */
  def removeFile(fileNames: String*): Unit = {
    delta(storagePath) match {
      case Some(dt) =>
        import spark.implicits._
        val toRemove = fileNames.toDF("filename")
        dt.as("target")
          .merge(toRemove.as("source"), "target.filename = source.filename")
          .whenMatched()
          .delete()
          .execute()
        _files = null
        logger.warn(s"Removed ${fileNames.size} files from FileList $name")
      case None =>
        logger.warn(s"FileList $name does not exist, nothing to remove")
    }
  }

  /** Checks if a specific file is tracked in this file list.
    *
    * @param fileName
    *   the file path to check
    * @return
    *   true if the file exists in the list
    */
  def hasFile(fileName: String): Boolean =
    !files.filter(col("filename") === fileName).isEmpty

  /** Persists the current in-memory file list to the Delta table.
    *
    * Uses a merge (upsert) if the table already exists, or a full overwrite for
    * first-time creation. Invalidates the in-memory cache after writing.
    */
  private def write: Unit = {
    delta(storagePath) match {
      case Some(delta) =>
        delta
          .as("target")
          .merge(files.as("source"), "target.filename = source.filename")
          .whenMatched()
          .updateExpr(Map("addedAt" -> "target.addedAt"))
          .whenNotMatched()
          .insertAll()
          .execute()
      case None =>
        files.write
          .format("delta")
          .mode("overwrite")
          .save(storagePath.toString)
    }
    _files = null
    logger.warn(s"Wrote out FileList $name")
  }

}

/** Factory and utility methods for FileList instances.
  *
  * Provides path resolution, existence checks, and removal operations for file
  * lists stored as Delta tables.
  */
object FileList {

  /** Returns the base storage path for all file lists.
    *
    * @param sparkSession
    *   the implicit SparkSession
    * @return
    *   Hadoop Path to the filelists directory
    */
  def storagePath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "filelists")
  }

  /** Checks if a file list with the given name exists on storage.
    *
    * @param name
    *   the file list name
    * @param sparkSession
    *   the implicit SparkSession
    * @return
    *   true if the Delta table directory exists
    */
  def exists(name: String)(implicit sparkSession: SparkSession): Boolean = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    contextUser.exists(new Path(storagePath(sparkSession), name))
  }

  /** Removes a file list's Delta table from storage.
    *
    * @param name
    *   the file list name
    * @param sparkSession
    *   the implicit SparkSession
    * @return
    *   true if deletion was successful
    * @throws FileListNotFoundException
    *   if the file list does not exist
    */
  def remove(name: String)(implicit sparkSession: SparkSession): Boolean = {
    if (!exists(name)(sparkSession)) {
      throw new FileListNotFoundException(name)
    }
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    contextUser.delete(new Path(storagePath(sparkSession), name))
  }

  /** Creates a new [[FileList]] instance for the given name.
    *
    * @param name
    *   the file list name
    * @param spark
    *   the implicit SparkSession
    * @return
    *   a new FileList backed by a Delta table at `storagePath/name`
    */
  def apply(name: String)(implicit spark: SparkSession): FileList = {
    new FileList(name)(spark)
  }
}
