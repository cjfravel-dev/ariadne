package dev.cjfravel.ariadne

import java.io.FileNotFoundException
import java.net.URI

import dev.cjfravel.ariadne.exceptions.InvalidDeltaTableException
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}

/**
 * Test-only filesystem that simulates a path deleted by another process between an existence check and the inspection
 * that follows it.
 *
 * Only the directory literally named `vanished_dir` is affected, so every other path — including the `_delta_log` probe
 * performed by Delta — behaves normally. Instances are created by Hadoop through a no-argument constructor and hold no
 * mutable state, so they are safe for concurrent test lookups.
 */
class VanishingPathFileSystem extends RawLocalFileSystem {
  override def getUri: URI = URI.create("vanishing:///")

  private def hasVanished(path: Path): Boolean = path.getName == "vanished_dir"

  override def exists(path: Path): Boolean =
    hasVanished(path) || super.exists(path)

  override def getFileStatus(path: Path): FileStatus =
    if (hasVanished(path)) throw new FileNotFoundException(s"Injected vanished path: $path")
    else super.getFileStatus(path)
}

/**
 * Tests for [[AriadneContextUser]] verifying storage path configuration is correctly read from the SparkSession, and
 * that Delta table resolution never destroys data it does not recognise.
 */
class AriadneContextTests extends SparkTests {

  private def contextUser: AriadneContextUser =
    new AriadneContextUser {
      implicit def spark: org.apache.spark.sql.SparkSession =
        AriadneContextTests.this.spark
    }

  test("storagePath") {
    assert(contextUser.storagePath.toString === tempDir.toString)
  }

  test("Spark test session uses bounded parallelism for tiny fixtures") {
    assert(spark.conf.get("spark.sql.shuffle.partitions") === "1")
    assert(spark.conf.get("spark.databricks.delta.snapshotPartitions") === "1")
    assert(spark.sparkContext.defaultParallelism === 4)
    assert(!spark.sparkContext.getConf.getBoolean("spark.ui.enabled", true))
  }

  test("delta returns None for an absent path") {
    assert(contextUser.delta(new Path(tempDir.toString, "delta_absent")).isEmpty)
  }

  test("delta treats an empty directory as an absent table without deleting it") {
    val path = new Path(tempDir.toString, "delta_empty_dir")
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(path)

    assert(contextUser.delta(path).isEmpty)
    assert(fs.exists(path), "an empty directory must be left in place")
  }

  test("delta refuses to delete a non-empty path that is not a Delta table") {
    val path = new Path(tempDir.toString, "delta_foreign_data")
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(path)
    val payload = new Path(path, "someone_elses.txt")
    val stream = fs.create(payload, true)
    try stream.writeUTF("not ariadne's data")
    finally stream.close()

    val thrown =
      intercept[InvalidDeltaTableException] {
        contextUser.delta(path)
      }
    assert(thrown.getMessage.contains(path.toString))
    assert(fs.exists(payload), "the offending path must be left untouched for the caller to inspect")
  }

  test("delta reports a path deleted mid-inspection as absent rather than failing") {
    // delta() checks existence, then inspects. Another process can delete the path in between; a concurrently
    // deleted path is an absent path, which delta already contracts to return None for.
    val storageKey = "spark.ariadne.storagePath"
    val originalStoragePath = spark.conf.get(storageKey)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.vanishing.impl", classOf[VanishingPathFileSystem].getName)
    hadoopConf.setBoolean("fs.vanishing.impl.disable.cache", true)
    val vanishingStoragePath = s"vanishing://${tempDir.toAbsolutePath}"
    spark.conf.set(storageKey, vanishingStoragePath)

    try {
      assert(contextUser.delta(new Path(s"$vanishingStoragePath/vanished_dir")).isEmpty)
    } finally {
      spark.conf.set(storageKey, originalStoragePath)
      hadoopConf.unset("fs.vanishing.impl")
      hadoopConf.unset("fs.vanishing.impl.disable.cache")
      org.apache.hadoop.fs.FileSystem.closeAll()
    }
  }
}
