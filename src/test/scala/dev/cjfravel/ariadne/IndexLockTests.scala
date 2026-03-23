package dev.cjfravel.ariadne

import com.google.gson.Gson
import dev.cjfravel.ariadne.exceptions.IndexLockException
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.io.Source

/** Tests for [[IndexLock]] covering lock acquisition, release, refresh,
  * stale lock auto-healing, and contention behavior.
  */
class IndexLockTests extends SparkTests {

  private val gson = new Gson()

  private def fileSystem: FileSystem =
    FileSystem.get(tempDir.toUri, spark.sparkContext.hadoopConfiguration)

  private def lockPath(testName: String): Path =
    new Path(new Path(tempDir.toUri), s"index-lock-$testName.json")

  private def cleanup(path: Path): Unit = {
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

  private def readLock(path: Path): LockInfo = {
    val in = fileSystem.open(path)
    try {
      val json = Source.fromInputStream(in)(StandardCharsets.UTF_8).mkString
      gson.fromJson(json, classOf[LockInfo])
    } finally {
      in.close()
    }
  }

  private def writeLock(path: Path, info: LockInfo): Unit = {
    val out = fileSystem.create(path, true)
    try {
      out.write(gson.toJson(info).getBytes(StandardCharsets.UTF_8))
      out.flush()
    } finally {
      out.close()
    }
  }

  private def withConfigOverrides[T](overrides: (String, String)*)(block: => T): T = {
    val originals = overrides.map { case (key, _) => key -> spark.conf.getOption(key) }
    overrides.foreach { case (key, value) => spark.conf.set(key, value) }
    try block
    finally {
      originals.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None)        => spark.conf.unset(key)
      }
    }
  }

  test("acquire and release lock") {
    val path = lockPath("acquire-release")
    cleanup(path)
    val lock = IndexLock(path, "test-index")
    val correlationId = "corr-1"

    try {
      lock.acquire(correlationId)
      assert(fileSystem.exists(path))
      lock.release(correlationId)
      assert(!fileSystem.exists(path))
    } finally {
      cleanup(path)
    }
  }

  test("acquire lock is idempotent for same correlation ID after release") {
    val path = lockPath("idempotent")
    cleanup(path)
    val lock = IndexLock(path, "test-index")
    val correlationId = "same-id"

    try {
      lock.acquire(correlationId)
      lock.release(correlationId)
      lock.acquire(correlationId)
      assert(fileSystem.exists(path))
      lock.release(correlationId)
    } finally {
      cleanup(path)
    }
  }

  test("release with wrong correlation ID does not remove lock") {
    val path = lockPath("wrong-release")
    cleanup(path)
    val lock = IndexLock(path, "test-index")

    try {
      lock.acquire("a")
      lock.release("b")
      assert(fileSystem.exists(path))
      lock.release("a")
      assert(!fileSystem.exists(path))
    } finally {
      cleanup(path)
    }
  }

  test("refresh updates lastRefreshedAt timestamp") {
    val path = lockPath("refresh-updates")
    cleanup(path)
    val lock = IndexLock(path, "test-index")
    val correlationId = "refresh-id"

    try {
      lock.acquire(correlationId)
      val before = readLock(path)
      Thread.sleep(100)
      lock.refresh(correlationId)
      val after = readLock(path)
      assert(after.lastRefreshedAt != before.lastRefreshedAt)
      assert(after.acquiredAt == before.acquiredAt)
      lock.release(correlationId)
    } finally {
      cleanup(path)
    }
  }

  test("refresh with wrong correlation ID does not update lock") {
    val path = lockPath("refresh-wrong-id")
    cleanup(path)
    val lock = IndexLock(path, "test-index")

    try {
      lock.acquire("a")
      val before = readLock(path)
      lock.refresh("b")
      val after = readLock(path)
      assert(after.lastRefreshedAt == before.lastRefreshedAt)
      lock.release("a")
    } finally {
      cleanup(path)
    }
  }

  test("stale lock is auto-healed") {
    val path = lockPath("stale-auto-heal")
    cleanup(path)
    val staleTime = Instant.now().minusSeconds(3600).toString
    writeLock(path, LockInfo("a", staleTime, staleTime, "stale-owner"))
    val lock = IndexLock(path, "test-index")
    val newCorrelationId = "b"

    try {
      lock.acquire(newCorrelationId)
      val info = readLock(path)
      assert(info.correlationId == newCorrelationId)
      lock.release(newCorrelationId)
    } finally {
      cleanup(path)
    }
  }

  test("lock contention throws IndexLockException after max wait") {
    val path = lockPath("lock-contention")
    cleanup(path)

    try {
      withConfigOverrides(
        "spark.ariadne.lockMaxWait" -> "1",
        "spark.ariadne.lockRetryInterval" -> "1"
      ) {
        val lockA = IndexLock(path, "test-index")
        val lockB = IndexLock(path, "test-index")

        lockA.acquire("a")
        try {
          intercept[IndexLockException] {
            lockB.acquire("b")
          }
        } finally {
          lockA.release("a")
        }
      }
    } finally {
      cleanup(path)
    }
  }
}
