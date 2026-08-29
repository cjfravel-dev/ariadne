package dev.cjfravel.ariadne

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests pinning how files are classified as "large" for a given indexed column and how large column values are
 * persisted.
 *
 * Classification is driven by the number of *distinct* values a file contributes to a column, never by its row count.
 * These tests guard that contract and the resulting on-disk layout so the build path can stream large columns straight
 * into `large_indexes/{column}` without first materializing a per-file array.
 */
class LargeIndexClassificationTests extends SparkTests with Matchers {

  private val testSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Version", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false)))

  /** Runs `body` with `spark.ariadne.largeIndexLimit` temporarily set to `limit`. */
  private def withLargeIndexLimit[T](limit: Long)(body: => T): T = {
    val previous = spark.conf.getOption("spark.ariadne.largeIndexLimit")
    spark.conf.set("spark.ariadne.largeIndexLimit", limit.toString)
    try body
    finally
      previous match {
        case Some(v) => spark.conf.set("spark.ariadne.largeIndexLimit", v)
        case None => spark.conf.unset("spark.ariadne.largeIndexLimit")
      }
  }

  /** Reads the main index Delta table for `index`. */
  private def mainIndex(index: Index) =
    spark.read.format("delta").load(new Path(index.storagePath, "index").toString)

  /** Returns the `large_indexes/{column}` table for `index`, or `None` when the column has no large index table. */
  private def largeIndex(index: Index, column: String) = {
    val path = new Path(new Path(index.storagePath, "large_indexes"), column)
    if (index.exists(path)) Some(spark.read.format("delta").load(path.toString)) else None
  }

  /** Writes `rows` as a single CSV file and returns its path. */
  private def writeCsv(name: String, rows: Seq[Row]): String = {
    val dir = s"${System.getProperty("java.io.tmpdir")}/$name-${System.currentTimeMillis()}"
    spark
      .createDataFrame(spark.sparkContext.parallelize(rows), testSchema)
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(dir)
    dir
  }

  test("a column with many duplicate rows but few distinct values is not classified as large") {
    // table1_part0.csv has 4 rows: Id has 3 distinct values, Version has only 2 across those 4 rows.
    // With a limit of 3, a row-count based rule would wrongly push Version into the large index.
    withLargeIndexLimit(3) {
      val index = Index("large_class_dupes", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.addIndex("Version")
      index.update

      largeIndex(index, "Version") shouldBe None

      val row = mainIndex(index).select("Id", "Version").head()
      row.isNullAt(0) shouldBe true // Id: 3 distinct >= 3, stored in large_indexes
      row.getSeq[Int](1).toSet shouldBe Set(1, 2) // Version: 2 distinct < 3, stays inline
    }
  }

  test("large column values are stored as distinct filename/value rows and nulled inline") {
    withLargeIndexLimit(3) {
      val index = Index("large_class_storage", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      mainIndex(index).select("Id").head().isNullAt(0) shouldBe true

      val large = largeIndex(index, "Id").getOrElse(fail("expected large index table for Id"))
      large.columns should contain allOf ("filename", "Id")
      // Id 1 appears on two source rows but must be stored once.
      large.select("Id").collect().map(_.getInt(0)).toSeq.sorted shouldBe Seq(1, 2, 3)
      large.count() shouldBe 3L
    }
  }

  test("largeness is decided per file, not per column, within a single batch") {
    // fileA contributes 5 distinct Ids, fileB only 2. With a limit of 4 only fileA is large.
    val bigPath = writeCsv("large_class_big", (1 to 5).map(i => Row(i, 1, i.toDouble)))
    val smallPath = writeCsv("large_class_small", Seq(Row(100, 1, 1.0), Row(101, 1, 2.0), Row(100, 2, 3.0)))

    withLargeIndexLimit(4) {
      val index = Index("large_class_per_file", testSchema, "csv", Map("header" -> "true"))
      index.addFile(bigPath)
      index.addFile(smallPath)
      index.addIndex("Id")
      index.update

      val inline =
        mainIndex(index)
          .select("filename", "Id")
          .collect()
          .map(r => (r.getString(0), if (r.isNullAt(1)) None else Some(r.getSeq[Int](1).toSet)))
          .toMap

      val bigFile = inline.keys.find(_.contains("large_class_big")).getOrElse(fail("big file missing from index"))
      val smallFile = inline.keys.find(_.contains("large_class_small")).getOrElse(fail("small file missing"))

      inline(bigFile) shouldBe None // streamed to large_indexes
      inline(smallFile) shouldBe Some(Set(100, 101)) // stays inline

      val large = largeIndex(index, "Id").getOrElse(fail("expected large index table for Id"))
      large.select("filename").distinct().collect().map(_.getString(0)).toSeq shouldBe Seq(bigFile)
      large.select("Id").collect().map(_.getInt(0)).toSeq.sorted shouldBe (1 to 5)
    }
  }

  test("query results are identical whether a column is stored inline or as a large index") {
    val queryDf =
      spark
        .createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1), Row(3), Row(4), Row(99))),
          StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    def resultsFor(indexName: String, limit: Long): Set[(Int, Int)] =
      withLargeIndexLimit(limit) {
        val index = Index(indexName, testSchema, "csv", Map("header" -> "true"))
        index.addFile(resourcePath("/data/table1_part0.csv"))
        index.addFile(resourcePath("/data/table1_part1.csv"))
        index.addIndex("Id")
        index.update
        index
          .join(queryDf, Seq("Id"), "inner")
          .select("Id", "Version")
          .collect()
          .map(r => (r.getInt(0), r.getInt(1)))
          .toSet
      }

    val inlineResults = resultsFor("large_class_parity_inline", 500000)
    val largeResults = resultsFor("large_class_parity_large", 2)

    largeResults shouldBe inlineResults
    inlineResults should not be empty
  }

  test("temporal index values survive the large index path") {
    val temporalSchema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = false),
          StructField("Value", DoubleType, nullable = false),
          StructField("UpdatedAt", TimestampType, nullable = false)))

    withLargeIndexLimit(2) {
      val index = Index("large_class_temporal", temporalSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/temporal_part0.csv"))
      index.addFile(resourcePath("/data/temporal_part1.csv"))
      index.addTemporalIndex("Id", "UpdatedAt")
      index.update

      val large = largeIndex(index, "Id").getOrElse(fail("expected large index table for temporal column Id"))
      // Struct shape must be preserved: (value, max_ts) per filename.
      large.schema("Id").dataType shouldBe a[StructType]
      large.select(col("Id.value")).collect().map(_.getInt(0)).distinct.sorted should contain allOf (1, 2, 3)

      // Temporal dedup still returns exactly one row per Id, taking the newest version.
      val queryDf =
        spark
          .createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
            StructType(Seq(StructField("Id", IntegerType, nullable = false))))
      val joined = index.join(queryDf, Seq("Id"), "inner").select("Id").collect().map(_.getInt(0))
      joined.toSeq.sorted shouldBe joined.distinct.toSeq.sorted
    }
  }

  test("exploded field index values survive the large index path") {
    val explodedSchema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(
              Seq(StructField("id", LongType, nullable = false), StructField("name", StringType, nullable = true)))),
            nullable = true)))

    val tempPath = s"${System.getProperty("java.io.tmpdir")}/large_class_exploded_${System.currentTimeMillis()}"
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row("evt1", Array(Row(100L, "Alice"), Row(101L, "Bob"), Row(102L, "Carol"))),
            Row("evt2", Array(Row(100L, "Alice"), Row(103L, "Dave"))))),
        explodedSchema)
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(tempPath)

    try
      withLargeIndexLimit(3) {
        val index = Index("large_class_exploded", explodedSchema, "json", Map.empty[String, String])
        index.addFile(tempPath)
        index.addExplodedFieldIndex("users", "id", "user_id")
        index.update

        val large = largeIndex(index, "user_id").getOrElse(fail("expected large index table for user_id"))
        large.select("user_id").collect().map(_.getLong(0)).toSeq.sorted shouldBe Seq(100L, 101L, 102L, 103L)

        val queryDf =
          spark
            .createDataFrame(
              spark.sparkContext.parallelize(Seq(Row(101L))),
              StructType(Seq(StructField("user_id", LongType, nullable = false))))
        index.join(queryDf, Seq("user_id"), "inner").count() should be > 0L
      }
    finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(tempPath), true)
    }
  }

  test("re-indexing a file replaces rather than duplicates its large index rows") {
    withLargeIndexLimit(3) {
      val index = Index("large_class_reindex", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      // Adding a second index column forces a backfill that re-processes the already indexed file.
      index.addIndex("Version")
      index.update

      val large = largeIndex(index, "Id").getOrElse(fail("expected large index table for Id"))
      large.count() shouldBe 3L
    }
  }

  test("a regular column is not lost when the index also has an exploded field index") {
    // analyzeFiles applies applyExplodedFields, whose inner `explode` drops rows with a null array.
    // event_id has 6 distinct values, but only 3 survive the explode. If classification is measured
    // on the exploded rows while the array is built from all rows, the column is neither stored
    // inline nor written to large_indexes and the values disappear entirely.
    val explodedSchema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(
              Seq(StructField("id", LongType, nullable = false), StructField("name", StringType, nullable = true)))),
            nullable = true)))

    val tempPath = s"${System.getProperty("java.io.tmpdir")}/large_class_mixed_${System.currentTimeMillis()}"
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("evt1", Array(Row(100L, "Alice"))),
          Row("evt2", Array(Row(101L, "Bob"))),
          Row("evt3", Array(Row(102L, "Carol"))),
          Row("evt4", null),
          Row("evt5", null),
          Row("evt6", null))),
        explodedSchema)
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(tempPath)

    try
      withLargeIndexLimit(5) {
        val index = Index("large_class_mixed", explodedSchema, "json", Map.empty[String, String])
        index.addFile(tempPath)
        index.addIndex("event_id")
        index.addExplodedFieldIndex("users", "id", "user_id")
        index.update

        // All six ids must be recorded, whether inline or in large_indexes, and remain locatable.
        val stored =
          largeIndex(index, "event_id") match {
            case Some(large) => large.select("event_id").collect().map(_.getString(0)).toSet
            case None =>
              mainIndex(index).select("event_id").collect().flatMap(r => r.getSeq[String](0)).toSet
          }
        stored shouldBe Set("evt1", "evt2", "evt3", "evt4", "evt5", "evt6")

        // evt6 has a null users array, so it only survives if event_id was classified on the
        // unexploded rows.
        index.locateFiles(Map("event_id" -> Array("evt6"))) should have size 1
      }
    finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(tempPath), true)
    }
  }

  test("an exploded column is measured on its own array, not one shared with another exploded index") {
    // applyExplodedFields folds an inner `explode` over EVERY configured array, so a row with a
    // null `users` is dropped before `tags` is counted. buildExplodedFieldIndexes builds each
    // column from its own array in isolation, so tag_name's stored array holds all 6 values while
    // a shared-plan analysis would see only 3 -- under-reporting the count that decides largeness
    // and drives batch packing.
    val multiSchema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", LongType, nullable = false)))),
            nullable = true),
          StructField(
            "tags",
            ArrayType(StructType(Seq(StructField("name", StringType, nullable = false)))),
            nullable = true)))

    val tempPath = s"${System.getProperty("java.io.tmpdir")}/large_class_multi_${System.currentTimeMillis()}"
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("evt1", Array(Row(100L)), Array(Row("t1"))),
          Row("evt2", Array(Row(101L)), Array(Row("t2"))),
          Row("evt3", Array(Row(102L)), Array(Row("t3"))),
          // users is null here, so these rows vanish from any plan that explodes both arrays.
          Row("evt4", null, Array(Row("t4"))),
          Row("evt5", null, Array(Row("t5"))),
          Row("evt6", null, Array(Row("t6"))))),
        multiSchema)
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(tempPath)

    try
      withLargeIndexLimit(4) {
        val index = Index("large_class_multi", multiSchema, "json", Map.empty[String, String])
        index.addFile(tempPath)
        index.addExplodedFieldIndex("users", "id", "user_id")
        index.addExplodedFieldIndex("tags", "name", "tag_name")
        index.update

        // tag_name has 6 distinct values, at or above the limit of 4, so it must be classified
        // large. A shared-plan analysis sees only the 3 rows that survive the `users` explode and
        // would leave it inline.
        val tagLarge = largeIndex(index, "tag_name").getOrElse(fail("expected large index table for tag_name"))
        tagLarge.select("tag_name").collect().map(_.getString(0)).toSet shouldBe
          Set("t1", "t2", "t3", "t4", "t5", "t6")

        // user_id has only 3 distinct values, below the limit, so it stays inline.
        largeIndex(index, "user_id") shouldBe None
        mainIndex(index).select("user_id").collect().flatMap(r => r.getSeq[Long](0)).toSet shouldBe
          Set(100L, 101L, 102L)

        // Values must remain locatable from whichever store holds them.
        index.locateFiles(Map("tag_name" -> Array("t6"))) should have size 1
        index.locateFiles(Map("user_id" -> Array(100L))) should have size 1
      }
    finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(tempPath), true)
    }
  }

  test("a file with no exploded values is still analyzed when every index column is exploded") {
    // With only exploded storage columns there is no direct pass, so the analysis is built purely
    // from exploded rows. `explode` yields nothing for a null array, so such a file would be absent
    // from analyzeFiles entirely, createOptimalBatches would never schedule it, and update would
    // silently skip the file.
    val onlyExplodedSchema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", LongType, nullable = false)))),
            nullable = true)))

    val base = s"${System.getProperty("java.io.tmpdir")}/large_class_allnull_${System.currentTimeMillis()}"
    val withValues = s"$base/with_values"
    val withoutValues = s"$base/without_values"
    spark
      .createDataFrame(spark.sparkContext.parallelize(Seq(Row("evt1", Array(Row(100L))))), onlyExplodedSchema)
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(withValues)
    spark
      .createDataFrame(spark.sparkContext.parallelize(Seq(Row("evt2", null))), onlyExplodedSchema)
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(withoutValues)

    try
      withLargeIndexLimit(4) {
        val index = Index("large_class_allnull", onlyExplodedSchema, "json", Map.empty[String, String])
        index.addFile(withValues)
        index.addFile(withoutValues)
        index.addExplodedFieldIndex("users", "id", "user_id")
        index.update

        // Both files must be represented in the index; the one with no exploded values simply
        // carries a null array rather than being dropped from the analysis.
        val indexed = mainIndex(index).select("filename").collect().map(_.getString(0))
        indexed should have size 2

        index.locateFiles(Map("user_id" -> Array(100L))) should have size 1
      }
    finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(base), true)
    }
  }

  test("a temporal column with null values is not lost at the large index boundary") {
    // collect_set keeps one struct for the null value group, so the stored array is one longer than
    // countDistinct reports. At the boundary that mismatch must not drop the file's values.
    val temporalSchema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = true),
          StructField("Value", DoubleType, nullable = false),
          StructField("UpdatedAt", TimestampType, nullable = false)))

    val dir = java.nio.file.Files.createTempDirectory("large_class_temporal_null")
    val csv = dir.resolve("data.csv")
    java.nio.file.Files.write(
      csv,
      Seq(
        "Id,Value,UpdatedAt",
        "1,100.0,2024-01-15 10:00:00",
        "2,200.0,2024-01-15 10:00:00",
        ",300.0,2024-01-15 10:00:00").mkString("\n").getBytes("UTF-8"))

    try
      withLargeIndexLimit(3) {
        val index = Index("large_class_temporal_null", temporalSchema, "csv", Map("header" -> "true"))
        index.addFile("file://" + csv.toString)
        index.addTemporalIndex("Id", "UpdatedAt")
        index.update

        val queryDf =
          spark
            .createDataFrame(
              spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
              StructType(Seq(StructField("Id", IntegerType, nullable = false))))
        index.join(queryDf, Seq("Id"), "inner").count() shouldBe 2L
      }
    finally {
      java.nio.file.Files.deleteIfExists(csv)
      java.nio.file.Files.deleteIfExists(dir)
    }
  }

  test("a file that stops being large has its stale large index rows removed") {
    // Indexed while large (5 distinct Ids, limit 4), then rewritten in place with only 2 distinct Ids.
    val dir = java.nio.file.Files.createTempDirectory("large_class_shrink")
    val csv = dir.resolve("data.csv")
    def write(ids: Seq[Int]): Unit =
      java.nio.file.Files
        .write(csv, ("Id,Version,Value" +: ids.map(i => s"$i,1,$i.0")).mkString("\n").getBytes("UTF-8"))

    try
      withLargeIndexLimit(4) {
        write(1 to 5)

        val index = Index("large_class_shrink", testSchema, "csv", Map("header" -> "true"))
        index.addFile("file://" + csv.toString)
        index.addIndex("Id")
        index.update

        largeIndex(index, "Id").map(_.count()) shouldBe Some(5L)

        // Shrink the source, then force a re-read of the already indexed file via a backfill.
        write(Seq(1, 2))
        index.addIndex("Version")
        index.update

        // The file is no longer large for Id, so its rows must not linger in large_indexes.
        largeIndex(index, "Id").map(_.count()) shouldBe Some(0L)
        mainIndex(index).select("Id").head().getSeq[Int](0).toSet shouldBe Set(1, 2)
      }
    finally {
      java.nio.file.Files.deleteIfExists(csv)
      java.nio.file.Files.deleteIfExists(dir)
    }
  }
}
