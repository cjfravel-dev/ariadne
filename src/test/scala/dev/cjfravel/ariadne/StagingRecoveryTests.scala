package dev.cjfravel.ariadne

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import com.google.gson.Gson
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests deterministic recovery of staging rows left by interrupted updates.
 */
class StagingRecoveryTests extends SparkTests with Matchers {

  private val gson = new Gson()

  private val sourceSchema =
    StructType(Seq(StructField("Id", IntegerType, nullable = false)))

  private def indexPath(indexName: String, child: String): String =
    new Path(new Path(IndexPathUtils.storagePath, indexName), child).toString

  private def readLock(path: Path): LockInfo = {
    val input = path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)
    try {
      gson.fromJson(new String(input.readAllBytes(), StandardCharsets.UTF_8), classOf[LockInfo])
    } finally {
      input.close()
    }
  }

  test("internal staging column names are reserved") {
    val index =
      Index(
        "reserved_staging_column",
        StructType(Seq(StructField("_ariadne_staged_at", TimestampType, nullable = false))),
        "parquet")

    an[IllegalArgumentException] should be thrownBy index.addIndex("_ariadne_staged_at")
    an[IllegalArgumentException] should be thrownBy index.addBloomIndex("_ariadne_staged_at")
    an[IllegalArgumentException] should be thrownBy index.addRangeIndex("_ariadne_staged_at")
  }

  test("stale staging recovery refreshes the update lock") {
    val indexName = "staging_recovery_heartbeat"
    val index = Index(indexName, sourceSchema, "parquet")
    index.addIndex("Id")
    val stagingSchema =
      StructType(
        Seq(
          StructField("filename", StringType, nullable = false),
          StructField("Id", ArrayType(IntegerType), nullable = true),
          StructField("file_size", LongType, nullable = false)))
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("file:///tmp/staging-heartbeat.parquet", Seq(1), 10L))),
        stagingSchema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(indexPath(indexName, "staging"))
    val lockPath = new Path(index.storagePath, ".update.lock")
    val lock = IndexLock(lockPath, indexName)
    val correlationId = "staging-recovery-owner"
    lock.acquire(correlationId)
    val before = readLock(lockPath)

    try {
      index.recoverStagingUnderLock(lock, correlationId)
      readLock(lockPath).lastRefreshedAt should not be before.lastRefreshedAt
    } finally {
      lock.release(correlationId)
    }
  }

  test("update consolidates stale staging when no files are pending") {
    val indexName = "stale_staging_recovery"
    val index = Index(indexName, sourceSchema, "parquet")
    index.addIndex("Id")
    val filename = "file:///tmp/stale-staging.parquet"
    val mainSchema =
      StructType(
        Seq(
          StructField("filename", StringType, nullable = false),
          StructField("Id", ArrayType(IntegerType), nullable = true),
          StructField("file_size", LongType, nullable = false)))
    spark
      .createDataFrame(spark.sparkContext.parallelize(Seq(Row(filename, Seq(1), 10L))), mainSchema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(indexPath(indexName, "index"))

    val stagingSchema =
      mainSchema
        .add("_ariadne_staged_at", TimestampType, nullable = false)
        .add("_ariadne_batch_id", StringType, nullable = false)
    val older = Timestamp.valueOf("2026-01-01 00:00:00")
    val newer = Timestamp.valueOf("2026-01-02 00:00:00")
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row(filename, Seq(2), 10L, older, "batch-a"), Row(filename, Seq(3), 10L, newer, "batch-b"))),
        stagingSchema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(indexPath(indexName, "staging"))

    index.update

    val result = spark.read.format("delta").load(indexPath(indexName, "index"))
    result.select("Id").head().getSeq[Int](0) shouldBe Seq(3)
    result.columns should not contain "_ariadne_staged_at"
    result.columns should not contain "_ariadne_batch_id"
    new Path(indexPath(indexName, "staging"))
      .getFileSystem(spark.sparkContext.hadoopConfiguration)
      .exists(new Path(indexPath(indexName, "staging"))) shouldBe false
  }

  test("legacy staging deterministically prefers complete rows") {
    val indexName = "legacy_staging_recovery"
    val index = Index(indexName, sourceSchema, "parquet")
    index.addIndex("Id")
    val filename = "file:///tmp/legacy-staging.parquet"
    val stagingSchema =
      StructType(
        Seq(
          StructField("filename", StringType, nullable = false),
          StructField("Id", ArrayType(IntegerType), nullable = true),
          StructField("file_size", LongType, nullable = false)))
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(filename, null, 10L), Row(filename, Seq(7), 10L))),
        stagingSchema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(indexPath(indexName, "staging"))

    index.update

    val result = spark.read.format("delta").load(indexPath(indexName, "index"))
    result.select("Id").head().getSeq[Int](0) shouldBe Seq(7)
  }
}
