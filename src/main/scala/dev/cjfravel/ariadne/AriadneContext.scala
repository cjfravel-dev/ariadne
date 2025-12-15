package dev.cjfravel.ariadne

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.logging.log4j.{Logger, LogManager}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.FSDataInputStream

/** Provides context and helper methods for Ariadne operations using an implicit
  * SparkSession. This trait should be mixed in by classes that need access to
  * Ariadne resources.
  */
trait AriadneContextUser {
  val logger: Logger = LogManager.getLogger("ariadne")

  /** Implicit SparkSession that must be provided by the mixing class */
  implicit def spark: SparkSession

  /** Path on FileSystem where Ariadne should create files. Reads from
    * spark.ariadne.storagePath configuration.
    */
  lazy val storagePath: Path = {
    val path = new Path(spark.conf.get("spark.ariadne.storagePath"))
    logger.warn(s"storagePath initialized: $path")
    println(s"Ariadne storage path: $path")
    path
  }

  /** Maximum number of records before an index is considered "large". Reads
    * from spark.ariadne.largeIndexLimit configuration (default: 500000).
    */
  lazy val largeIndexLimit: Long = {
    val limit = spark.conf.get("spark.ariadne.largeIndexLimit", "500000").toLong
    logger.warn(s"largeIndexLimit initialized: $limit")
    limit
  }

  /** Threshold for using broadcast join filtering instead of isin() predicates.
    * When the number of distinct values exceeds this threshold, we use broadcast joins.
    * Reads from spark.ariadne.broadcastJoinThreshold configuration (default: 10000).
    */
  lazy val broadcastJoinThreshold: Long = {
    val threshold = spark.conf.get("spark.ariadne.broadcastJoinThreshold", "10000").toLong
    logger.warn(s"broadcastJoinThreshold initialized: $threshold")
    threshold
  }

  /** Number of batches to process before consolidating staged data into the main index.
    * This provides fault tolerance for large index builds - if a job fails, work is preserved
    * up to the last consolidation point. Reads from spark.ariadne.stagingConsolidationThreshold
    * configuration (default: 50).
    */
  lazy val stagingConsolidationThreshold: Int = {
    val threshold = spark.conf.get("spark.ariadne.stagingConsolidationThreshold", "50").toInt
    logger.warn(s"stagingConsolidationThreshold initialized: $threshold")
    threshold
  }

  /** Hadoop FileSystem instance associated with the storage path. */
  lazy val fs: FileSystem = {
    val filesystem = FileSystem.get(
      storagePath.getParent.toUri,
      spark.sparkContext.hadoopConfiguration
    )
    logger.warn(s"FileSystem initialized for: ${storagePath.getParent}")
    filesystem
  }

  /** Checks if a path exists on the filesystem */
  def exists(path: Path): Boolean = fs.exists(path)

  /** Deletes a path recursively from the filesystem */
  def delete(path: Path): Boolean = fs.delete(path, true)

  /** Opens an input stream to read from a path */
  def open(path: Path): FSDataInputStream = fs.open(path)

  /** Returns a DeltaTable if the path exists and is a valid Delta table. If the
    * path exists but is not a valid Delta table, it will be deleted and None
    * returned. If the path doesn't exist, None is returned.
    */
  def delta(path: Path): Option[DeltaTable] = {
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
}
