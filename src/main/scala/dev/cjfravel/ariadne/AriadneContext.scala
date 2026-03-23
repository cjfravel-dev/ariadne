package dev.cjfravel.ariadne

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.logging.log4j.{Logger, LogManager}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.FSDataInputStream

/** Provides Spark session context, HDFS access, and configuration for Ariadne operations.
  *
  * Mix this trait into any class that needs access to Ariadne infrastructure.
  * All configuration is read lazily from `SparkConf` properties prefixed with
  * `spark.ariadne.*` and logged at `warn` level on first access.
  *
  * '''Spark configuration keys:'''
  * {{{
  * spark.ariadne.storagePath                 (required) Base HDFS/filesystem path for index storage
  * spark.ariadne.largeIndexLimit             (default: 500000) Max values before a column is "large"
  * spark.ariadne.stagingConsolidationThreshold (default: 50) Batches between staging merges
  * spark.ariadne.indexRepartitionCount       (optional) Repartition count for index DataFrames
  * spark.ariadne.debug                       (default: false) Enable verbose join diagnostics
  * spark.ariadne.repartitionDataFiles        (default: false) Repartition data files during joins
  * spark.ariadne.lockTimeout                 (default: 1800) Seconds before a lock is considered stale
  * spark.ariadne.lockRetryInterval           (default: 60) Base seconds between lock retries
  * spark.ariadne.lockMaxWait                 (default: 3600) Max seconds to wait for a lock
  * spark.ariadne.lockRefreshInterval         (default: 1) Batches between lock refreshes
  * spark.ariadne.autoCompactThreshold        (optional) Delta log file count triggering auto-compact
  * spark.ariadne.autoBloomFpr                (default: 0.01) FPR for auto-bloom filters on large columns
  * }}}
  *
  * '''Thread safety:''' Lazy vals are initialized at most once per instance and
  * are safe for concurrent reads after initialization. However, the underlying
  * `SparkSession` and `FileSystem` are shared resources governed by Spark's own
  * concurrency model.
  */
trait AriadneContextUser {
  /** Log4j logger shared by all Ariadne components. Uses the "ariadne" logger name. */
  val logger: Logger = LogManager.getLogger("ariadne")

  /** Implicit SparkSession that must be provided by the mixing class. */
  implicit def spark: SparkSession

  /** Base path on the Hadoop-compatible filesystem where all Ariadne index data
    * is stored. Each index creates a subdirectory under this path.
    *
    * Reads from `spark.ariadne.storagePath` (required — no default).
    *
    * @throws IllegalArgumentException if the configuration key is not set
    */
  lazy val storagePath: Path = {
    val pathStr = spark.conf.getOption("spark.ariadne.storagePath")
      .getOrElse(throw new IllegalArgumentException(
        "Spark configuration 'spark.ariadne.storagePath' must be set. " +
        "Set it via spark.conf.set(\"spark.ariadne.storagePath\", \"/path/to/storage\")"))
    val path = new Path(pathStr)
    logger.warn(s"storagePath initialized: $path")
    path
  }

  /** Maximum number of distinct values per file before an index column is promoted
    * to a "large index" (stored in a separate exploded Delta table). Reads
    * from `spark.ariadne.largeIndexLimit` (default: 500000).
    *
    * If the configured value is not a valid positive number, a warning is logged
    * and the default of 500,000 is used.
    */
  lazy val largeIndexLimit: Long = {
    val limit = try {
      spark.conf.get("spark.ariadne.largeIndexLimit", "500000").toLong
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.largeIndexLimit value, using default 500000")
        500000L
    }
    val validated = if (limit <= 0) {
      logger.warn(s"spark.ariadne.largeIndexLimit must be positive (got $limit), using default 500000")
      500000L
    } else {
      limit
    }
    logger.warn(s"largeIndexLimit initialized: $validated")
    validated
  }

  /** Number of batches to process before consolidating staged data into the
    * main index. This provides fault tolerance for large index builds - if a
    * job fails, work is preserved up to the last consolidation point. Reads
    * from spark.ariadne.stagingConsolidationThreshold configuration (default:
    * 50).
    */
  lazy val stagingConsolidationThreshold: Int = {
    val threshold = try {
      spark.conf.get("spark.ariadne.stagingConsolidationThreshold", "50").toInt
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.stagingConsolidationThreshold value, using default 50")
        50
    }
    logger.warn(s"stagingConsolidationThreshold initialized: $threshold")
    threshold
  }

  /** Optional number of partitions to use when repartitioning the index
    * DataFrame during joins. When set, the index DataFrame is repartitioned
    * before expensive operations like explode to reduce per-executor memory
    * pressure and avoid FetchFailedExceptions on large indexes. Reads from
    * spark.ariadne.indexRepartitionCount configuration (default: not set).
    */
  lazy val indexRepartitionCount: Option[Int] = {
    val count = spark.conf.getOption("spark.ariadne.indexRepartitionCount").flatMap { v =>
      try {
        Some(v.toInt)
      } catch {
        case _: NumberFormatException =>
          logger.warn("Invalid spark.ariadne.indexRepartitionCount value, ignoring")
          None
      }
    }
    count.foreach(c => logger.warn(s"indexRepartitionCount initialized: $c"))
    count
  }

  /** When true, logs detailed diagnostics during join operations including
    * partition counts, physical plans, and per-phase timing. Reads from
    * spark.ariadne.debug configuration (default: false).
    */
  lazy val debugEnabled: Boolean = {
    val enabled = try {
      spark.conf.get("spark.ariadne.debug", "false").toBoolean
    } catch {
      case _: IllegalArgumentException =>
        logger.warn("Invalid spark.ariadne.debug value, using default false")
        false
    }
    if (enabled) logger.warn("Debug mode enabled")
    enabled
  }

  /** When true, applies indexRepartitionCount repartitioning to the
    * data files read during joinDf. When false, data files keep their natural
    * parquet partitioning. Disable when column selection significantly reduces
    * data volume, making the repartition shuffle more expensive than useful.
    * Reads from spark.ariadne.repartitionDataFiles configuration (default: false).
    */
  lazy val repartitionDataFiles: Boolean = {
    val enabled = try {
      spark.conf.get("spark.ariadne.repartitionDataFiles", "false").toBoolean
    } catch {
      case _: IllegalArgumentException =>
        logger.warn("Invalid spark.ariadne.repartitionDataFiles value, using default false")
        false
    }
    if (enabled) logger.warn("repartitionDataFiles enabled — data files will be repartitioned")
    enabled
  }

  /** Hadoop `FileSystem` instance resolved from the [[storagePath]] URI and
    * the Spark Hadoop configuration. Used for all filesystem operations
    * (existence checks, reads, deletes, lock files).
    */
  lazy val fs: FileSystem = {
    val fsUri = Option(storagePath.getParent).getOrElse(storagePath).toUri
    val filesystem = FileSystem.get(
      fsUri,
      spark.sparkContext.hadoopConfiguration
    )
    logger.warn(s"FileSystem initialized for: $storagePath")
    filesystem
  }

  /** Checks if a path exists on the filesystem.
    *
    * @param path The Hadoop Path to check
    * @return true if the path exists
    */
  def exists(path: Path): Boolean = fs.exists(path)

  /** Deletes a path recursively from the filesystem.
    *
    * @param path The Hadoop Path to delete
    * @return true if the path was successfully deleted
    */
  def delete(path: Path): Boolean = fs.delete(path, true)

  /** Opens an input stream to read from a path on the filesystem.
    *
    * @param path The Hadoop Path to open for reading
    * @return An FSDataInputStream for reading the file contents
    */
  def open(path: Path): FSDataInputStream = fs.open(path)

  /** Returns a [[io.delta.tables.DeltaTable]] if the path exists and contains valid Delta metadata.
    *
    * If the path exists but is ''not'' a valid Delta table (e.g., leftover partial
    * write), the directory is deleted and `None` is returned so it can be
    * recreated cleanly on the next write.
    *
    * @param path The Hadoop Path to check for a Delta table
    * @return `Some(DeltaTable)` if a valid Delta table exists, `None` otherwise
    */
  def delta(path: Path): Option[DeltaTable] = {
    if (exists(path)) {
      if (DeltaTable.isDeltaTable(spark, path.toString)) {
        Some(DeltaTable.forPath(spark, path.toString))
      } else {
        logger.warn(s"Path $path exists but is not a Delta table — deleting to allow re-creation")
        delete(path)
        None
      }
    } else {
      None
    }
  }

  /** Seconds since `lastRefreshedAt` before a lock is considered stale and
    * eligible for auto-heal (forcible acquisition by another process).
    * Reads from `spark.ariadne.lockTimeout` (default: 1800).
    */
  lazy val lockTimeout: Long = {
    val value = try {
      spark.conf.get("spark.ariadne.lockTimeout", "1800").toLong
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.lockTimeout value, using default 1800")
        1800L
    }
    logger.warn(s"lockTimeout initialized: $value")
    value
  }

  /** Base interval in seconds between lock acquisition retries (exponential backoff applied).
    * Reads from spark.ariadne.lockRetryInterval configuration (default: 60).
    */
  lazy val lockRetryInterval: Long = {
    val value = try {
      spark.conf.get("spark.ariadne.lockRetryInterval", "60").toLong
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.lockRetryInterval value, using default 60")
        60L
    }
    logger.warn(s"lockRetryInterval initialized: $value")
    value
  }

  /** Maximum total time in seconds to wait for lock acquisition before failing.
    * Reads from spark.ariadne.lockMaxWait configuration (default: 3600).
    */
  lazy val lockMaxWait: Long = {
    val value = try {
      spark.conf.get("spark.ariadne.lockMaxWait", "3600").toLong
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.lockMaxWait value, using default 3600")
        3600L
    }
    logger.warn(s"lockMaxWait initialized: $value")
    value
  }

  /** Refresh the update lock every N batches during updateBatched.
    * Reads from spark.ariadne.lockRefreshInterval configuration (default: 1).
    */
  lazy val lockRefreshInterval: Int = {
    val value = try {
      spark.conf.get("spark.ariadne.lockRefreshInterval", "1").toInt
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.lockRefreshInterval value, using default 1")
        1
    }
    logger.warn(s"lockRefreshInterval initialized: $value")
    value
  }

  /** Optional threshold for auto-compaction. When set, the main index Delta table
    * is compacted after an update if the number of Delta log JSON files meets or
    * exceeds this value. Reads from spark.ariadne.autoCompactThreshold configuration
    * (default: not set).
    */
  lazy val autoCompactThreshold: Option[Int] = {
    val value = spark.conf.getOption("spark.ariadne.autoCompactThreshold").flatMap { v =>
      try {
        Some(v.toInt)
      } catch {
        case _: NumberFormatException =>
          logger.warn("Invalid spark.ariadne.autoCompactThreshold value, ignoring")
          None
      }
    }
    value.foreach(v => logger.warn(s"autoCompactThreshold initialized: $v"))
    value
  }

  /** False positive rate for auto-bloom filters on large index columns.
    * When a column exceeds largeIndexLimit, an auto-bloom filter is built
    * with this FPR to enable file pruning at query time.
    * Reads from spark.ariadne.autoBloomFpr configuration (default: 0.01).
    */
  lazy val autoBloomFpr: Double = {
    val value = try {
      spark.conf.get("spark.ariadne.autoBloomFpr", "0.01").toDouble
    } catch {
      case _: NumberFormatException =>
        logger.warn("Invalid spark.ariadne.autoBloomFpr value, using default 0.01")
        0.01
    }
    logger.warn(s"autoBloomFpr initialized: $value")
    value
  }
}
