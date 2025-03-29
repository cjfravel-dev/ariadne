package dev.cjfravel.ariadne

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.logging.log4j.{Logger, LogManager}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.FSDataInputStream

/** Represents an Context for tracking
  *
  * @param spark
  *   The SparkSession instance.
  */
object AriadneContext {
  val logger = LogManager.getLogger("ariadne")

  private var _spark: SparkSession = _
  private var _fs: FileSystem = _
  private var _storagePath: Path = _
  private var _updateFileLimit: Int = _

  def setSparkSession(spark: SparkSession): Unit = {
    logger.trace("spark set")
    _spark = spark
    _storagePath = new Path(
      spark.conf.get("spark.ariadne.storagePath")
    )
    _fs = FileSystem.get(
      _storagePath.getParent.toUri,
      spark.sparkContext.hadoopConfiguration
    )
    _updateFileLimit = spark.conf.get("spark.ariadne.updateFileLimit", "100").toInt
  }

  /** SparkSession associated with the running job */
  private[ariadne] def spark: SparkSession = _spark

  /** Hadoop FileSystem instance associated with the storage path. */
  private[ariadne] def fs: FileSystem = _fs
  private[ariadne] def exists(path: Path): Boolean = fs.exists(path)
  private[ariadne] def delete(path: Path): Boolean = fs.delete(path, true)
  private[ariadne] def open(path: Path): FSDataInputStream = fs.open(path)

  /** Path on FileSystem where ariadre should create file */
  private[ariadne] def storagePath: Path = _storagePath
  private[ariadne] def delta(path: Path): Option[DeltaTable] = {
    if (exists(path)) {
      if (DeltaTable.isDeltaTable(spark, path.toString)) {
        Some(DeltaTable.forPath(spark, path.toString))
      } else {
        delete(path)
        None
      }
    } else {
      None
    }
  }

  private[ariadne] def updateFileLimit: Int = _updateFileLimit
}

private[ariadne] trait AriadneContextUser {
  def spark: SparkSession = AriadneContext.spark

  def fs: FileSystem = AriadneContext.fs
  def exists(path: Path): Boolean = AriadneContext.exists(path)
  def delete(path: Path): Boolean = AriadneContext.delete(path)
  def open(path: Path): FSDataInputStream = AriadneContext.open(path)

  def storagePath: Path = AriadneContext.storagePath
  def delta(path: Path): Option[DeltaTable] = AriadneContext.delta(path)
  
  def updateFileLimit: Int = AriadneContext.updateFileLimit
}
