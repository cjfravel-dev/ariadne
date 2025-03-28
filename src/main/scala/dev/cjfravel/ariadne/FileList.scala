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
import collection.JavaConverters._
import java.util
import java.util.Collections
import org.apache.logging.log4j.{Logger, LogManager}
import org.apache.spark.sql.Dataset

import java.time.Instant
import java.sql.Timestamp

case class FileEntry(filename: String, addedAt: java.sql.Timestamp)

case class FileList private (
    name: String
) {
  val logger = LogManager.getLogger("ariadne")

  private def storagePath: Path = new Path(FileList.storagePath, name)

  private var _files: Dataset[FileEntry] = _

  private def files(spark: SparkSession): Dataset[FileEntry] = {
    if (_files == null) {
      import spark.implicits._
      _files = Context.delta(storagePath) match {
        case Some(delta) => delta.toDF.as[FileEntry]
        case None        => spark.emptyDataset[FileEntry]
      }
    }

    _files
  }

  def files: Dataset[FileEntry] = files(Context.spark)

  private def addFile(spark: SparkSession, fileNames: String*): Unit = {
    import spark.implicits._
    val existing = files.map(_.filename).collect().toSet
    val toAdd = fileNames.toSet.diff(existing)
    if (toAdd.isEmpty) {
      logger.warn("All files were already added")
      return
    }

    val ts = Timestamp.from(Instant.now())
    val newFiles =
      toAdd.toList
        .map { FileEntry(_, ts) }
        .toDS()

    _files = _files.union(newFiles)
    write
    logger.trace(s"Added ${toAdd.size} files")
  }

  def addFile(fileNames: String*): Unit = addFile(Context.spark, fileNames: _*)

  def hasFile(fileName: String): Boolean =
    !files.filter(col("filename") === fileName).isEmpty

  private def write: Unit = {
    Context.delta(storagePath) match {
      case Some(delta) =>
        delta
          .as("target")
          .merge(files.toDF.as("source"), "target.filename = source.filename")
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
    logger.trace(s"Wrote out FileList $name")
  }

}

object FileList {
  def storagePath: Path = new Path(Context.storagePath, "filelists")

  def exists(name: String): Boolean =
    Context.exists(new Path(storagePath, name))

  def remove(name: String): Boolean = {
    if (!exists(name)) {
      throw new FileListNotFoundException(name)
    }

    Context.delete(new Path(storagePath, name))
  }
}
