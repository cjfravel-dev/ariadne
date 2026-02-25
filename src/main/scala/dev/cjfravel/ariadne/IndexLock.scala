package dev.cjfravel.ariadne

import com.google.gson.Gson
import dev.cjfravel.ariadne.exceptions.IndexLockException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import scala.io.Source

case class LockInfo(
    correlationId: String,
    acquiredAt: String,
    lastRefreshedAt: String,
    owner: String
)

case class IndexLock(lockPath: Path, indexName: String)(implicit val spark: SparkSession)
    extends AriadneContextUser {

  private val gson = new Gson()

  private def getOwner: String = {
    try {
      spark.sparkContext.applicationId
    } catch {
      case _: Exception => InetAddress.getLocalHost.getHostName
    }
  }

  /** Acquires the lock for the given correlation ID.
    * @param correlationId
    *   The correlation ID to associate with the lock.
    */
  def acquire(correlationId: String): Unit = {
    val startTime = System.currentTimeMillis()
    var attempt = 0

    def tryAcquire(): Unit = {
      try {
        val now = Instant.now().toString
        val lockInfo = LockInfo(correlationId, now, now, getOwner)
        val outputStream = fs.create(lockPath, false)
        try {
          outputStream.write(gson.toJson(lockInfo).getBytes(StandardCharsets.UTF_8))
          outputStream.flush()
        } finally {
          outputStream.close()
        }
      } catch {
        case _: org.apache.hadoop.fs.FileAlreadyExistsException | _: java.io.IOException =>
          handleExistingLock(correlationId, startTime, attempt)
      }
    }

    tryAcquire()
  }

  private def handleExistingLock(correlationId: String, startTime: Long, attempt: Int): Unit = {
    readLock() match {
      case Some(existingLock) if isStale(existingLock) =>
        logger.warn(
          s"Auto-healing stale lock for index '$indexName' " +
            s"(held by correlationId='${existingLock.correlationId}', owner='${existingLock.owner}')"
        )
        delete(lockPath)
        acquire(correlationId)
      case Some(existingLock) =>
        retryLoop(correlationId, startTime, attempt, existingLock)
      case None =>
        acquire(correlationId)
    }
  }

  private def retryLoop(
      correlationId: String,
      startTime: Long,
      attempt: Int,
      lastLock: LockInfo
  ): Unit = {
    var currentAttempt = attempt
    var currentLock = lastLock

    while (true) {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      if (elapsed >= lockMaxWait) {
        throw new IndexLockException(indexName, currentLock.correlationId, currentLock.owner)
      }

      val sleepSeconds = Math.min(lockRetryInterval * Math.pow(2, currentAttempt).toLong, 60)
      Thread.sleep(sleepSeconds * 1000)
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
            val lockInfo = LockInfo(correlationId, now, now, getOwner)
            val outputStream = fs.create(lockPath, false)
            try {
              outputStream.write(gson.toJson(lockInfo).getBytes(StandardCharsets.UTF_8))
              outputStream.flush()
            } finally {
              outputStream.close()
            }
            return
          } catch {
            case _: org.apache.hadoop.fs.FileAlreadyExistsException | _: java.io.IOException =>
              currentLock = readLock().getOrElse(currentLock)
          }
        case Some(lock) =>
          currentLock = lock
        case None =>
          try {
            val now = Instant.now().toString
            val lockInfo = LockInfo(correlationId, now, now, getOwner)
            val outputStream = fs.create(lockPath, false)
            try {
              outputStream.write(gson.toJson(lockInfo).getBytes(StandardCharsets.UTF_8))
              outputStream.flush()
            } finally {
              outputStream.close()
            }
            return
          } catch {
            case _: org.apache.hadoop.fs.FileAlreadyExistsException | _: java.io.IOException =>
              currentLock = readLock().getOrElse(currentLock)
          }
      }
    }
  }

  /** Releases the lock for the given correlation ID.
    * @param correlationId
    *   The correlation ID that holds the lock.
    */
  def release(correlationId: String): Unit = {
    readLock() match {
      case Some(lockInfo) if lockInfo.correlationId == correlationId =>
        delete(lockPath)
      case Some(lockInfo) =>
        logger.warn(
          s"Cannot release lock for index '$indexName': " +
            s"correlationId mismatch (expected='$correlationId', actual='${lockInfo.correlationId}')"
        )
      case None =>
        logger.warn(s"Cannot release lock for index '$indexName': lock file does not exist")
    }
  }

  /** Refreshes the lock timestamp for the given correlation ID.
    * @param correlationId
    *   The correlation ID that holds the lock.
    */
  def refresh(correlationId: String): Unit = {
    readLock() match {
      case Some(lockInfo) if lockInfo.correlationId == correlationId =>
        val updated = lockInfo.copy(lastRefreshedAt = Instant.now().toString)
        writeLock(updated)
      case Some(lockInfo) =>
        logger.warn(
          s"Cannot refresh lock for index '$indexName': " +
            s"correlationId mismatch (expected='$correlationId', actual='${lockInfo.correlationId}')"
        )
      case None =>
        logger.warn(s"Cannot refresh lock for index '$indexName': lock file does not exist")
    }
  }

  private def isStale(lockInfo: LockInfo): Boolean = {
    try {
      val lastRefreshed = Instant.parse(lockInfo.lastRefreshedAt)
      Duration.between(lastRefreshed, Instant.now()).getSeconds > lockTimeout
    } catch {
      case _: Exception => true
    }
  }

  private def readLock(): Option[LockInfo] = {
    try {
      if (exists(lockPath)) {
        val inputStream = open(lockPath)
        try {
          val jsonString = Source.fromInputStream(inputStream)(StandardCharsets.UTF_8).mkString
          Some(gson.fromJson(jsonString, classOf[LockInfo]))
        } finally {
          inputStream.close()
        }
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  private def writeLock(lockInfo: LockInfo): Unit = {
    val outputStream = fs.create(lockPath, true)
    try {
      outputStream.write(gson.toJson(lockInfo).getBytes(StandardCharsets.UTF_8))
      outputStream.flush()
    } finally {
      outputStream.close()
    }
  }
}
