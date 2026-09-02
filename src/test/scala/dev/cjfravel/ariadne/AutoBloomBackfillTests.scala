package dev.cjfravel.ariadne

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.gson.{JsonObject, JsonParser}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers

/**
 * Tests for backfilling auto-bloom filters onto indexes whose `large_indexes/` tables were written before the column
 * was registered as auto-bloom, and for treating an executed but empty auto-bloom probe as a definitive no-match.
 */
class AutoBloomBackfillTests extends SparkTests with Matchers {

  val testSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Version", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false)))

  val temporalSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true)))

  private def metadataPath(index: Index): Path = new Path(index.storagePath, "metadata.json")

  private def readMetadataJson(index: Index): JsonObject = {
    val path = metadataPath(index)
    val input = path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)
    try {
      JsonParser.parseString(new String(input.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject
    } finally {
      input.close()
    }
  }

  private def writeMetadataJson(index: Index, json: JsonObject): Unit = {
    val path = metadataPath(index)
    val output = path.getFileSystem(spark.sparkContext.hadoopConfiguration).create(path, true)
    try {
      output.write(json.toString.getBytes(StandardCharsets.UTF_8))
    } finally {
      output.close()
    }
  }

  private def indexTablePath(index: Index): String = new Path(index.storagePath, "index").toString

  private def indexTable(index: Index): DataFrame = spark.read.format("delta").load(indexTablePath(index))

  /**
   * Rewrites an index so it looks like one built before `column` was auto-bloom: the filter column is dropped from the
   * main table and the column is removed from `auto_bloom_indexes`. The `large_indexes/` table is left untouched.
   *
   * @param storageVersion
   *   value to record as `storage_format_version`, defaulting to the version that preceded the backfill
   */
  private def stripAutoBloom(
      index: Index,
      column: String,
      storageVersion: Int = StorageFormat.ExplodedFieldStorageVersion): Unit = {
    val bloomColumn = s"auto_bloom_$column"
    indexTable(index)
      .drop(bloomColumn)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(indexTablePath(index))

    val json = readMetadataJson(index)
    val remaining = new com.google.gson.JsonArray()
    Option(json.getAsJsonArray("auto_bloom_indexes")).foreach { existing =>
      existing.iterator().asScala.foreach(entry => if (entry.getAsString != column) remaining.add(entry))
    }
    json.add("auto_bloom_indexes", remaining)
    json.addProperty("storage_format_version", storageVersion)
    writeMetadataJson(index, json)
  }

  private def withSmallLargeIndexLimit[T](body: => T): T =
    try {
      spark.conf.set("spark.ariadne.largeIndexLimit", "1")
      body
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }

  test("backfill adds an auto-bloom filter to an index whose large index predates it") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_regular", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      indexTable(index).columns should contain("auto_bloom_Id")
      stripAutoBloom(index, "Id")
      indexTable(index).columns should not contain "auto_bloom_Id"

      // Any operation that runs migration preflight triggers the backfill.
      Index("backfill_regular", testSchema, "csv", Map("header" -> "true"))
        .locateFiles(Map("Id" -> Array[Any](1))) should not be empty

      val reopened = Index("backfill_regular", testSchema, "csv", Map("header" -> "true"))
      indexTable(reopened).columns should contain("auto_bloom_Id")
      readMetadataJson(reopened)
        .getAsJsonArray("auto_bloom_indexes")
        .iterator()
        .asScala
        .map(_.getAsString)
        .toSet should contain("Id")
      readMetadataJson(reopened).get("storage_format_version").getAsInt shouldBe
        StorageFormat.AutoBloomBackfillStorageVersion
    }
  }

  test("backfilled filters match the values held in the large index") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_probe", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update
      stripAutoBloom(index, "Id")

      val reopened = Index("backfill_probe", testSchema, "csv", Map("header" -> "true"))
      reopened.locateFiles(Map("Id" -> Array[Any](1)))

      val probed = Index("backfill_probe", testSchema, "csv", Map("header" -> "true"))
      val indexDf = indexTable(probed)

      val present = probed.getAutoBloomCandidates("Id", Array[Any](1), indexDf)
      present shouldBe defined
      present.get should not be empty

      val absent = probed.getAutoBloomCandidates("Id", Array[Any](999999), indexDf)
      absent shouldBe defined
      absent.get shouldBe empty
    }
  }

  test("backfill covers temporal columns") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_temporal", temporalSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/temporal_part0.csv"))
      index.addFile(resourcePath("/data/temporal_part1.csv"))
      index.addTemporalIndex("Id", "UpdatedAt")
      index.update
      stripAutoBloom(index, "Id")

      val reopened = Index("backfill_temporal", temporalSchema, "csv", Map("header" -> "true"))
      reopened.locateFiles(Map("Id" -> Array[Any](1))) should not be empty

      val probed = Index("backfill_temporal", temporalSchema, "csv", Map("header" -> "true"))
      indexTable(probed).columns should contain("auto_bloom_Id")

      // The filter must be folded over the value field, not over the (value, max_ts) struct, or no probe can match.
      val present = probed.getAutoBloomCandidates("Id", Array[Any](1), indexTable(probed))
      present shouldBe defined
      present.get should not be empty
    }
  }

  test("backfill is idempotent and leaves no file without a filter") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_idempotent", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addFile(resourcePath("/data/table1_part1.csv"))
      index.addIndex("Id")
      index.update
      stripAutoBloom(index, "Id")

      (1 to 2).foreach { _ =>
        Index("backfill_idempotent", testSchema, "csv", Map("header" -> "true"))
          .locateFiles(Map("Id" -> Array[Any](1)))
      }

      val reopened = Index("backfill_idempotent", testSchema, "csv", Map("header" -> "true"))
      val table = indexTable(reopened)
      table.where(org.apache.spark.sql.functions.col("auto_bloom_Id").isNull).count() shouldBe 0
    }
  }

  test("a file whose values live only in the large index is still located after backfill") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_no_missed_files", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addFile(resourcePath("/data/table1_part1.csv"))
      index.addIndex("Id")
      index.update

      val before =
        Index("backfill_no_missed_files", testSchema, "csv", Map("header" -> "true"))
          .locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6)))

      stripAutoBloom(index, "Id")

      val after =
        Index("backfill_no_missed_files", testSchema, "csv", Map("header" -> "true"))
          .locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6)))

      after shouldBe before
      after should not be empty
    }
  }

  test("an executed but empty auto-bloom probe skips the large index instead of scanning it") {
    withSmallLargeIndexLimit {
      val index = Index("prune_empty_candidates", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      val largeDf = index.loadLargeIndex("Id")
      largeDf shouldBe defined

      index.pruneLargeIndexRows(largeDf.get, "Id", Some(Set.empty[String])) shouldBe None

      val unprobed = index.pruneLargeIndexRows(largeDf.get, "Id", None)
      unprobed shouldBe defined
      unprobed.get.count() shouldBe largeDf.get.count()
    }
  }

  test("pruning is skipped while a staging table exists") {
    withSmallLargeIndexLimit {
      val index = Index("prune_staging_guard", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      val largeDf = index.loadLargeIndex("Id").get
      index.autoBloomCandidatesAreExhaustive shouldBe true

      // Large index rows are written before the matching main index rows reach staging, so while staging exists a
      // candidate set derived from the main index is not an exhaustive allowlist.
      val stagingPath = new Path(index.storagePath, "staging")
      stagingPath.getFileSystem(spark.sparkContext.hadoopConfiguration).mkdirs(stagingPath)
      try {
        index.autoBloomCandidatesAreExhaustive shouldBe false
        val guarded = index.pruneLargeIndexRows(largeDf, "Id", Some(Set.empty[String]))
        guarded shouldBe defined
        guarded.get.count() shouldBe largeDf.count()
      } finally {
        stagingPath.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(stagingPath, true)
      }
    }
  }

  test("queries return the same rows before and after an empty probe skips the large index") {
    withSmallLargeIndexLimit {
      val index = Index("prune_query_parity", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addFile(resourcePath("/data/table1_part1.csv"))
      index.addIndex("Id")
      index.update

      index.locateFiles(Map("Id" -> Array[Any](999999))) shouldBe empty
      index.locateFiles(Map("Id" -> Array[Any](1))) should not be empty
    }
  }

  test("backfilled filters survive a subsequent update and staging consolidation") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_survives_update", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update
      stripAutoBloom(index, "Id")

      val backfilled = Index("backfill_survives_update", testSchema, "csv", Map("header" -> "true"))
      backfilled.locateFiles(Map("Id" -> Array[Any](1))) should not be empty
      indexTable(backfilled).columns should contain("auto_bloom_Id")

      val updated = Index("backfill_survives_update", testSchema, "csv", Map("header" -> "true"))
      updated.addFile(resourcePath("/data/table1_part1.csv"))
      updated.update

      val reopened = Index("backfill_survives_update", testSchema, "csv", Map("header" -> "true"))
      val table = indexTable(reopened)
      table.where(org.apache.spark.sql.functions.col("auto_bloom_Id").isNull).count() shouldBe 0
      table.count() shouldBe 2

      val present = reopened.getAutoBloomCandidates("Id", Array[Any](1), table)
      present shouldBe defined
      present.get should not be empty
      reopened.locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6))) should not be empty
    }
  }

  test("a filter missing despite a current version claim never drops files, and the next update repairs it") {
    withSmallLargeIndexLimit {
      val index = Index("backfill_version_claim", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      val expected = index.locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6)))
      expected should not be empty

      // Reads gate migration preflight on the recorded version alone, so this state survives a query untouched.
      stripAutoBloom(index, "Id", StorageFormat.AutoBloomBackfillStorageVersion)

      val queried = Index("backfill_version_claim", testSchema, "csv", Map("header" -> "true"))
      // A column absent from the main table yields no candidate set at all, so nothing is pruned and no file is lost.
      queried.locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6))) shouldBe expected
      indexTable(queried).columns should not contain "auto_bloom_Id"

      // Mutating operations run the content check rather than the version gate, so they repair the gap.
      val mutated = Index("backfill_version_claim", testSchema, "csv", Map("header" -> "true"))
      mutated.addFile(resourcePath("/data/table1_part1.csv"))
      mutated.update

      val reopened = Index("backfill_version_claim", testSchema, "csv", Map("header" -> "true"))
      indexTable(reopened).columns should contain("auto_bloom_Id")
      indexTable(reopened)
        .where(org.apache.spark.sql.functions.col("auto_bloom_Id").isNull)
        .count() shouldBe 0
      reopened.locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6))) should contain allElementsOf expected
    }
  }

  test("an exploded field large index that still uses the legacy inner column name migrates and stays queryable") {
    val arrayTestSchema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(
              StructType(Seq(
                StructField("id", IntegerType, nullable = false),
                StructField("name", StringType, nullable = false)))),
            nullable = false)))

    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/legacy_exploded_${System.currentTimeMillis()}"
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row("e1", Array(Row(100, "Alice"), Row(101, "Bob"))), Row("e2", Array(Row(102, "Charlie"))))),
        arrayTestSchema)
      .write
      .mode("overwrite")
      .parquet(sourcePath)

    val index =
      withSmallLargeIndexLimit {
        val built = Index("legacy_exploded_large", arrayTestSchema, "parquet")
        built.addFile(sourcePath)
        built.addExplodedFieldIndex("users", "id", "user_id")
        built.update
        built
      }

    val expected = index.locateFiles(Map("user_id" -> Array[Any](100)))
    expected should not be empty

    // Releases before 0.1.1-beta aliased exploded values to the array column name, and the exploded field migration
    // renames only the large index directory, so a migrated index can hold a table whose value column is still
    // 'users' inside 'large_indexes/user_id'.
    val largeIndexPath = new Path(new Path(index.storagePath, "large_indexes"), "user_id")
    spark.read
      .format("delta")
      .load(largeIndexPath.toString)
      .withColumnRenamed("user_id", "users")
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(largeIndexPath.toString)
    spark.read.format("delta").load(largeIndexPath.toString).columns should contain("users")

    stripAutoBloom(index, "user_id")

    val reopened = Index("legacy_exploded_large", arrayTestSchema, "parquet")
    reopened.locateFiles(Map("user_id" -> Array[Any](100))) shouldBe expected
    readMetadataJson(reopened).get("storage_format_version").getAsInt shouldBe
      StorageFormat.AutoBloomBackfillStorageVersion
  }

  test("backfill covers files whose values are stored inline as well as files in the large index") {
    val index =
      withSmallLargeIndexLimit {
        val first = Index("backfill_mixed", testSchema, "csv", Map("header" -> "true"))
        first.addFile(resourcePath("/data/table1_part0.csv"))
        first.addIndex("Id")
        first.update
        first
      }

    // A fresh instance reads the default limit, so this file's values stay inline in the main index.
    val later = Index("backfill_mixed", testSchema, "csv", Map("header" -> "true"))
    later.addFile(resourcePath("/data/table1_part1.csv"))
    later.update

    stripAutoBloom(index, "Id")

    val reopened = Index("backfill_mixed", testSchema, "csv", Map("header" -> "true"))
    reopened.locateFiles(Map("Id" -> Array[Any](1))) should not be empty

    val table = indexTable(Index("backfill_mixed", testSchema, "csv", Map("header" -> "true")))
    table.count() shouldBe 2
    table.where(org.apache.spark.sql.functions.col("auto_bloom_Id").isNull).count() shouldBe 0

    // With every file filtered, a value held by no file yields an empty candidate set, which is what makes the large
    // index skippable. A single null filter would keep its file a candidate and make the set non-empty.
    val probed = Index("backfill_mixed", testSchema, "csv", Map("header" -> "true"))
    val absent = probed.getAutoBloomCandidates("Id", Array[Any](999999), indexTable(probed))
    absent shouldBe defined
    absent.get shouldBe empty
    probed.locateFiles(Map("Id" -> Array[Any](999999))) shouldBe empty
  }

  test("a column registered as auto-bloom keeps getting filters when later batches hold no large file") {
    val index =
      withSmallLargeIndexLimit {
        val first = Index("backfill_recurrence", testSchema, "csv", Map("header" -> "true"))
        first.addFile(resourcePath("/data/table1_part0.csv"))
        first.addIndex("Id")
        first.update
        first
      }
    val largeIndexPath = new Path(new Path(index.storagePath, "large_indexes"), "Id")
    largeIndexPath.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(largeIndexPath) shouldBe true

    // A fresh instance reads the default limit, so no file in this batch is classified large for Id. Registration
    // lives in metadata rather than in the batch classification, so filters are still built.
    val later = Index("backfill_recurrence", testSchema, "csv", Map("header" -> "true"))
    later.addFile(resourcePath("/data/table1_part1.csv"))
    later.update

    val reopened = Index("backfill_recurrence", testSchema, "csv", Map("header" -> "true"))
    val table = indexTable(reopened)
    table.where(org.apache.spark.sql.functions.col("auto_bloom_Id").isNull).count() shouldBe 0
    reopened.locateFiles(Map("Id" -> Array[Any](1, 2, 3, 4, 5, 6))) should not be empty
  }
}
