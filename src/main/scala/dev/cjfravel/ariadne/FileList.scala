package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import com.google.gson._
import com.google.gson.reflect.TypeToken
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.collection.JavaConverters._
import java.util
import java.util.Collections
import org.apache.logging.log4j.{Logger, LogManager}
import java.time.Instant
import java.sql.Timestamp

/** Manages a tracked list of files associated with an Ariadne index.
  *
  * Files are stored as a Delta Lake table with filename and addedAt columns.
  * The file list is used to track which files have been registered with an index
  * and need to be processed during updates.
  *
  * @param name The name of this file list (typically "[ariadne_index] {indexName}")
  * @param spark Implicit SparkSession for Delta Lake operations
  */
case class FileList private (
    name: String
)(implicit val spark: SparkSession) extends AriadneContextUser {
  override val logger: Logger = LogManager.getLogger("ariadne")

  override lazy val storagePath: Path = new Path(FileList.storagePath, name)

  private var _files: DataFrame = _

  private def files(spark: SparkSession): DataFrame = {
    if (_files == null) {
      _files = delta(storagePath) match {
        case Some(delta) => delta.toDF
        case None        => spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
                              StructType(Seq(
                                StructField("filename", StringType, nullable = false),
                                StructField("addedAt", TimestampType, nullable = false)
                              )))
      }
    }

    _files
  }

  /** Returns the DataFrame of tracked files with filename and addedAt columns.
    * Lazily loaded from the Delta table on first access.
    * @return DataFrame with columns (filename: String, addedAt: Timestamp)
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
      StructType(Seq(
        StructField("filename", StringType, nullable = false),
        StructField("addedAt", TimestampType, nullable = false)
      ))
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

  /** Registers new files in the file list. Files already present are silently skipped.
    * @param fileNames One or more file paths to add
    */
  def addFile(fileNames: String*): Unit = addFile(spark, fileNames: _*)

  /** Removes files from the file list using a Delta merge-delete.
    * @param fileNames One or more file paths to remove
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
    * @param fileName The file path to check
    * @return true if the file exists in the list
    */
  def hasFile(fileName: String): Boolean =
    !files.filter(col("filename") === fileName).isEmpty

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
  * Provides path resolution, existence checks, and removal operations
  * for file lists stored as Delta tables.
  */
object FileList {
  /** Returns the base storage path for all file lists.
    * @return Hadoop Path to the filelists directory
    */
  def storagePath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "filelists")
  }

  /** Checks if a file list exists.
    * @param name The file list name
    * @return true if the Delta table exists
    */
  def exists(name: String)(implicit sparkSession: SparkSession): Boolean = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    contextUser.exists(new Path(storagePath(sparkSession), name))
  }

  /** Removes a file list's Delta table from storage.
    * @param name The file list name
    * @return true if deletion was successful
    * @throws FileListNotFoundException if the file list does not exist
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

  /** Creates a new FileList instance.
    * @param name The file list name
    * @return A new FileList instance
    */
  def apply(name: String)(implicit spark: SparkSession): FileList = {
    new FileList(name)(spark)
  }
}
