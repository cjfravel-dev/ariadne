package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.InvalidDeltaTableException
import org.apache.hadoop.fs.Path

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
}
