package dev.cjfravel.ariadne

/**
 * Tests for [[AriadneContextUser]] verifying storage path configuration is correctly read from the SparkSession.
 */
class AriadneContextTests extends SparkTests {
  test("storagePath") {
    val contextUser =
      new AriadneContextUser {
        implicit def spark: org.apache.spark.sql.SparkSession =
          AriadneContextTests.this.spark
      }
    assert(contextUser.storagePath.toString === tempDir.toString)
  }

  test("Spark test session uses bounded parallelism for tiny fixtures") {
    assert(spark.conf.get("spark.sql.shuffle.partitions") === "1")
    assert(spark.conf.get("spark.databricks.delta.snapshotPartitions") === "1")
    assert(spark.sparkContext.defaultParallelism === 4)
    assert(!spark.sparkContext.getConf.getBoolean("spark.ui.enabled", true))
  }
}
