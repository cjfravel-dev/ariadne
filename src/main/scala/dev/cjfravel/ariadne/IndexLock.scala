package dev.cjfravel.ariadne

import com.google.gson.Gson
import dev.cjfravel.ariadne.exceptions.IndexLockException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import scala.io.Source

/** Metadata stored in lock files to track lock ownership and freshness.
  *
  * Each lock file is a JSON document containing these fields, serialized via
  * Gson. The [[IndexLock]] reads and writes [[LockInfo]] instances to
  * coordinate distributed access to an index.
  *
  * @param correlationId
  *   Unique identifier for the lock holder's operation
  * @param acquiredAt
  *   ISO-8601 timestamp when the lock was first acquired
  * @param lastRefreshedAt
  *   ISO-8601 timestamp of the most recent lock refresh
  * @param owner
  *   Spark application ID, or the hostname if the application ID is unavailable
  */
case class LockInfo(
    correlationId: String,
    acquiredAt: String,
    lastRefreshedAt: String,
    owner: String
)

/** File-based distributed lock for index operations.
  *
  * Provides mutual exclusion for index operations (update, compact, vacuum)
  * using lock files on HDFS or cloud-compatible filesystems. Lock files are
  * JSON documents containing a [[LockInfo]] payload.
  *
  * ==Concurrency semantics==
  * Lock acquisition is atomic at the filesystem level: the lock file is created
  * with `overwrite = false`, so only one writer can succeed. If another process
  * already holds the lock, acquisition enters a retry loop with exponential
  * back-off (capped at 60 s) up to `lockMaxWait` seconds.
  *
  * ==Stale lock healing==
  * If the `lastRefreshedAt` timestamp is older than `lockTimeout` seconds, the
  * lock is considered stale. A stale lock is automatically deleted and
  * re-acquired, with a warning logged identifying the previous holder.
  *
  * ==Corrupt lock file handling==
  * If the lock file exists but cannot be parsed (empty or invalid JSON), it is
  * treated as corrupt: the file is deleted and acquisition is retried, up to
  * `MaxCorruptLockRetries` times to prevent infinite recursion.
  *
  * @param lockPath
  *   Path to the lock file on the filesystem
  * @param indexName
  *   Name of the index being locked (used in log messages)
  * @param spark
  *   Implicit SparkSession for filesystem access and configuration
  */
case class IndexLock(lockPath: Path, indexName: String)(implicit
    val spark: SparkSession
) extends AriadneContextUser {

  private val gson = new Gson()

  /** Maximum number of recursive acquire attempts when encountering corrupt or
    * transient lock files, to prevent stack overflow.
    */
  private val MaxCorruptLockRetries = 3

  /** Returns the Spark application ID, falling back to the local hostname.
    *
    * @return
    *   an owner identifier string for the lock file
    */
  private def getOwner: String = {
    try {
      spark.sparkContext.applicationId
    } catch {
      case e: Exception =>
        logger.debug(
          s"Could not get Spark application ID, falling back to hostname: ${e.getMessage}"
        )
        InetAddress.getLocalHost.getHostName
    }
  }

  /** Acquires the lock for the given correlation ID.
    *
    * If the lock file already exists, this method will either heal a stale or
    * corrupt lock, or enter a retry loop until the lock becomes available or
    * `lockMaxWait` is exceeded.
    *
    * @param correlationId
    *   unique identifier to associate with this lock hold
    * @throws IndexLockException
    *   if the lock cannot be acquired within the configured timeout or after
    *   max retry attempts
    */
  def acquire(correlationId: String): Unit = {
    require(correlationId != null && correlationId.trim.nonEmpty, "correlationId must not be null or blank")
    logger.debug(s"Acquiring lock for index '$indexName' (correlationId='$correlationId')")
    val acquireStart = System.currentTimeMillis()
    doAcquire(correlationId, depth = 0)
    logger.debug(
      s"Lock acquire completed for index '$indexName' in ${System.currentTimeMillis() - acquireStart}ms"
    )
  }

  /** Internal acquire implementation with a recursion depth guard.
    *
    * @param correlationId
    *   unique identifier to associate with this lock hold
    * @param depth
    *   current recursion depth (0-based)
    */
  private def doAcquire(correlationId: String, depth: Int): Unit = {
    if (depth >= MaxCorruptLockRetries) {
      throw new IndexLockException(
        s"Failed to acquire lock for index '$indexName' after $MaxCorruptLockRetries attempts " +
          s"(possible persistent corrupt lock file at $lockPath)"
      )
    }

    val startTime = System.currentTimeMillis()

    try {
      val now = Instant.now().toString
      writeLockFile(
        LockInfo(correlationId, now, now, getOwner),
        overwrite = false
      )
      logger.warn(
        s"Lock acquired for index '$indexName' (correlationId='$correlationId')"
      )
    } catch {
      case _: org.apache.hadoop.fs.FileAlreadyExistsException |
          _: java.io.IOException =>
        handleExistingLock(correlationId, startTime, 0, depth)
    }
  }

  /** Handles the case where a lock file already exists during acquisition.
    *
    * Reads the existing lock to determine whether it is stale (auto-heal),
    * active (enter retry loop), or corrupt/unreadable (delete and retry with
    * depth guard).
    *
    * '''TOCTOU note:''' There is an inherent race between reading the lock,
    * determining it is stale, deleting it, and re-creating it. A third process
    * could create a new lock between the delete and the re-acquire. The
    * `depth` guard limits recursion, and the filesystem's atomic-create
    * semantics (`overwrite = false`) ensure at most one writer succeeds.
    *
    * @param correlationId
    *   unique identifier for the new lock request
    * @param startTime
    *   epoch millis when the acquire attempt started
    * @param attempt
    *   current retry attempt number (for back-off)
    * @param depth
    *   current recursion depth to prevent infinite recursion
    */
  private def handleExistingLock(
      correlationId: String,
      startTime: Long,
      attempt: Int,
      depth: Int
  ): Unit = {
    readLock() match {
      case Some(existingLock) if isStale(existingLock) =>
        logger.warn(
          s"Auto-healing stale lock for index '$indexName' " +
            s"(held by correlationId='${existingLock.correlationId}', owner='${existingLock.owner}')"
        )
        delete(lockPath)
        doAcquire(correlationId, depth + 1)
      case Some(existingLock) =>
        retryLoop(correlationId, startTime, attempt, existingLock)
      case None =>
        if (exists(lockPath)) {
          logger.warn(
            s"Detected corrupt lock file for index '$indexName' at $lockPath, deleting"
          )
          delete(lockPath)
        }
        doAcquire(correlationId, depth + 1)
    }
  }

  /** Retry loop implementing exponential back-off for lock acquisition.
    *
    * Sleeps with exponential back-off (capped at 60 s) between attempts.
    * On each iteration the lock file is re-read to check for staleness or
    * release. The loop terminates when the lock is successfully acquired,
    * the caller is interrupted, or `lockMaxWait` is exceeded.
    *
    * @param correlationId
    *   unique identifier for the new lock request
    * @param startTime
    *   epoch millis when the acquire attempt started
    * @param attempt
    *   initial retry attempt number (for back-off calculation)
    * @param lastLock
    *   the most recently observed [[LockInfo]] from the lock file
    * @throws IndexLockException
    *   if the lock cannot be acquired within `lockMaxWait` seconds
    */
  private def retryLoop(
      correlationId: String,
      startTime: Long,
      attempt: Int,
      lastLock: LockInfo
  ): Unit = {
    var currentAttempt = attempt
    var currentLock = lastLock
    var acquired = false

    while (!acquired) {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      if (elapsed >= lockMaxWait) {
        throw new IndexLockException(
          indexName,
          currentLock.correlationId,
          currentLock.owner
        )
      }

      val sleepSeconds = math
        .min(
          lockRetryInterval.toDouble * math.pow(2, math.min(currentAttempt, 6)),
          60.0
        )
        .toLong
      logger.warn(
        s"Lock retry for index '$indexName': attempt=$currentAttempt, " +
          s"elapsed=${elapsed}s, sleeping=${sleepSeconds}s, " +
          s"holder=${currentLock.owner} (correlationId='${currentLock.correlationId}')"
      )
      try {
        Thread.sleep(sleepSeconds * 1000)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IndexLockException(
            s"Lock acquisition interrupted for index '$indexName'"
          )
      }
      currentAttempt += 1

      readLock() match {
        case Some(lock) if isStale(lock) =>
          logger.warn(
            s"Auto-healing stale lock for index '$indexName' " +
              s"(held by correlationId='${lock.correlationId}', owner='${lock.owner}')"
          )
          delete(lockPath)
          try {
            val now = Instant.now().toString
            writeLockFile(
              LockInfo(correlationId, now, now, getOwner),
              overwrite = false
            )
            logger.warn(
              s"Lock acquired for index '$indexName' (correlationId='$correlationId')"
            )
            acquired = true
          } catch {
            case _: org.apache.hadoop.fs.FileAlreadyExistsException |
                _: java.io.IOException =>
              currentLock = readLock().getOrElse(currentLock)
          }
        case Some(lock) =>
          currentLock = lock
        case None =>
          try {
            val now = Instant.now().toString
            writeLockFile(
              LockInfo(correlationId, now, now, getOwner),
              overwrite = false
            )
            logger.warn(
              s"Lock acquired for index '$indexName' (correlationId='$correlationId')"
            )
            acquired = true
          } catch {
            case _: org.apache.hadoop.fs.FileAlreadyExistsException |
                _: java.io.IOException =>
              currentLock = readLock().getOrElse(currentLock)
          }
      }
    }
  }

  /** Releases the lock if it is held by the given correlation ID.
    *
    * If the lock file does not exist or belongs to a different correlation ID,
    * a warning is logged and no action is taken.
    *
    * @param correlationId
    *   the correlation ID that should currently hold the lock
    */
  def release(correlationId: String): Unit = {
    require(correlationId != null && correlationId.trim.nonEmpty, "correlationId must not be null or blank")
    logger.debug(s"Releasing lock for index '$indexName' (correlationId='$correlationId')")
    val releaseStart = System.currentTimeMillis()
    readLock() match {
      case Some(lockInfo) if lockInfo.correlationId == correlationId =>
        delete(lockPath)
        logger.warn(
          s"Lock released for index '$indexName' (correlationId='$correlationId')"
        )
      case Some(lockInfo) =>
        logger.warn(
          s"Cannot release lock for index '$indexName': " +
            s"correlationId mismatch (expected='$correlationId', actual='${lockInfo.correlationId}')"
        )
      case None =>
        logger.warn(
          s"Cannot release lock for index '$indexName': lock file does not exist"
        )
    }
    logger.debug(
      s"Lock release completed for index '$indexName' in ${System.currentTimeMillis() - releaseStart}ms"
    )
  }

  /** Refreshes the lock's `lastRefreshedAt` timestamp to prevent stale-lock
    * healing.
    *
    * Long-running operations should call this periodically (more frequently
    * than `lockTimeout`) to signal that the lock holder is still active.
    *
    * @param correlationId
    *   the correlation ID that should currently hold the lock
    */
  def refresh(correlationId: String): Unit = {
    require(correlationId != null && correlationId.trim.nonEmpty, "correlationId must not be null or blank")
    logger.debug(s"Refreshing lock for index '$indexName' (correlationId='$correlationId')")
    val refreshStart = System.currentTimeMillis()
    readLock() match {
      case Some(lockInfo) if lockInfo.correlationId == correlationId =>
        val updated = lockInfo.copy(lastRefreshedAt = Instant.now().toString)
        writeLockFile(updated, overwrite = true)
        logger.warn(
          s"Lock refreshed for index '$indexName' (correlationId='$correlationId')"
        )
      case Some(lockInfo) =>
        logger.warn(
          s"Cannot refresh lock for index '$indexName': " +
            s"correlationId mismatch (expected='$correlationId', actual='${lockInfo.correlationId}')"
        )
      case None =>
        logger.warn(
          s"Cannot refresh lock for index '$indexName': lock file does not exist"
        )
    }
    logger.debug(
      s"Lock refresh completed for index '$indexName' in ${System.currentTimeMillis() - refreshStart}ms"
    )
  }

  /** Determines whether the lock is stale based on `lastRefreshedAt`.
    *
    * A lock is stale when the time elapsed since its last refresh exceeds the
    * configured `lockTimeout`. If the timestamp cannot be parsed, the lock is
    * considered stale and a warning is logged.
    *
    * @param lockInfo
    *   the lock metadata to evaluate
    * @return
    *   true if the lock should be considered stale
    */
  private def isStale(lockInfo: LockInfo): Boolean = {
    try {
      val lastRefreshed = Instant.parse(lockInfo.lastRefreshedAt)
      Duration.between(lastRefreshed, Instant.now()).getSeconds > lockTimeout
    } catch {
      case e: Exception =>
        logger.warn(
          s"Could not parse lastRefreshedAt='${lockInfo.lastRefreshedAt}' for index " +
            s"'$indexName', treating lock as stale: ${e.getMessage}"
        )
        true
    }
  }

  /** Reads and deserializes the lock file.
    *
    * @return
    *   `Some(lockInfo)` if the file exists and contains valid JSON, `None` if
    *   the file does not exist or cannot be parsed
    */
  private def readLock(): Option[LockInfo] = {
    try {
      if (exists(lockPath)) {
        val inputStream = open(lockPath)
        try {
          val source =
            Source.fromInputStream(inputStream)(StandardCharsets.UTF_8)
          try {
            val jsonString = source.mkString
            Option(gson.fromJson(jsonString, classOf[LockInfo]))
          } finally {
            source.close()
          }
        } finally {
          inputStream.close()
        }
      } else {
        None
      }
    } catch {
      case _: java.io.FileNotFoundException => None
      case e: com.google.gson.JsonSyntaxException =>
        logger.warn(s"Corrupt lock file at $lockPath: ${e.getMessage}")
        None
    }
  }

  /** Writes lock metadata to the lock file.
    *
    * @param lockInfo
    *   the metadata to serialize as JSON
    * @param overwrite
    *   if false, the write fails with `FileAlreadyExistsException` when the
    *   file already exists (atomic create)
    */
  private def writeLockFile(lockInfo: LockInfo, overwrite: Boolean): Unit = {
    var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null
    try {
      outputStream = fs.create(lockPath, overwrite)
      outputStream.write(gson.toJson(lockInfo).getBytes(StandardCharsets.UTF_8))
      outputStream.flush()
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to write lock file for index '$indexName': ${e.getMessage}"
        )
        throw e
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
  }
}
