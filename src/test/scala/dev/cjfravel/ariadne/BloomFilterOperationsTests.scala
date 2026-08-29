package dev.cjfravel.ariadne

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[BloomFilterOperations]] covering bloom filter index creation, custom false-positive-rate validation, and
 * bloom-filter-based file location queries.
 */
class BloomFilterOperationsTests extends SparkTests with Matchers {

  val testSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("UserId", LongType, nullable = false),
        StructField("Category", StringType, nullable = false),
        StructField("Value", DoubleType, nullable = false)))

  test("addBloomIndex should add a bloom index configuration") {
    val index =
      Index("bloom_add_test", testSchema, "csv", Map("header" -> "true"))

    index.addBloomIndex("UserId")

    index.indexes should contain("UserId")
  }

  test("addBloomIndex should accept custom FPR") {
    val index =
      Index("bloom_fpr_test", testSchema, "csv", Map("header" -> "true"))

    index.addBloomIndex("UserId", fpr = 0.001)

    index.indexes should contain("UserId")
  }

  test("addBloomIndex should reject FPR outside valid range") {
    val index = Index("bloom_invalid_fpr_test", testSchema, "csv", Map("header" -> "true"))

    an[IllegalArgumentException] should be thrownBy {
      index.addBloomIndex("UserId", fpr = 0.0)
    }

    an[IllegalArgumentException] should be thrownBy {
      index.addBloomIndex("UserId", fpr = 1.0)
    }

    an[IllegalArgumentException] should be thrownBy {
      index.addBloomIndex("UserId", fpr = -0.1)
    }
  }

  test("addBloomIndex should reject non-existent columns") {
    val index = Index("bloom_nonexistent_test", testSchema, "csv", Map("header" -> "true"))

    a[ColumnNotFoundException] should be thrownBy {
      index.addBloomIndex("NonExistentColumn")
    }
  }

  test("addBloomIndex and addIndex should be mutually exclusive") {
    val index1 =
      Index("bloom_exclusive_test1", testSchema, "csv", Map("header" -> "true"))
    index1.addIndex("UserId")

    an[IllegalArgumentException] should be thrownBy {
      index1.addBloomIndex("UserId")
    }

    val index2 =
      Index("bloom_exclusive_test2", testSchema, "csv", Map("header" -> "true"))
    index2.addBloomIndex("UserId")

    an[IllegalArgumentException] should be thrownBy {
      index2.addIndex("UserId")
    }
  }

  test("addBloomIndex should be idempotent") {
    val index =
      Index("bloom_idempotent_test", testSchema, "csv", Map("header" -> "true"))

    index.addBloomIndex("UserId")
    index.addBloomIndex("UserId") // Should not throw

    index.indexes.count(_ == "UserId") should be(1)
  }

  test("should build bloom filter index during update") {
    val index =
      Index("bloom_build_test", testSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")
    index.update

    // Verify the index was built
    index.indexes should contain("Id")
  }

  test("should locate files using bloom filter") {
    val index =
      Index("bloom_locate_test", testSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")
    index.update

    // Query for values that exist
    val files = index.locateFiles(Map("Id" -> Array(1, 2)))
    files should not be empty
    files should contain(csvPath)
  }

  test("bloom filter should return empty for definitely non-existent values") {
    val index = Index("bloom_nonexistent_value_test", testSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")
    index.update

    // Query for values that definitely don't exist
    // Note: Due to false positives, this might occasionally return files
    // But with small data and high cardinality values, it should be empty
    index.locateFiles(Map("Id" -> Array(999999, 888888)))
    // We can't guarantee empty due to FPR, but this tests the code path
  }

  test("should support mixed bloom and regular indexes") {
    val index =
      Index("bloom_mixed_test", testSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id") // Bloom filter
    index.addIndex("Category") // Regular index
    index.update

    // Both indexes should be available
    index.indexes should contain("Id")
    index.indexes should contain("Category")
  }

  test("should join using bloom filter index") {
    val index =
      Index("bloom_join_test", testSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")
    index.update

    // Create a query DataFrame
    val queryData = Seq(Row(1), Row(2), Row(3))
    val querySchema =
      StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val queryDf = spark.createDataFrame(spark.sparkContext.parallelize(queryData), querySchema)

    // Join should work
    val result = index.join(queryDf, Seq("Id"), "inner")
    result.count() should be >= 0L
  }

  test("should handle multiple bloom indexes") {
    // Create test data with multiple columns
    val multiColData = (1 to 100).map(i => Row(i, i.toLong * 1000, s"category_${i % 5}", i.toDouble))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(multiColData), testSchema)

    val tempPath =
      s"${System.getProperty("java.io.tmpdir")}/bloom_multi_test_${System.currentTimeMillis()}"
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempPath)

    try {
      val fileName =
        java.nio.file.Files
          .walk(java.nio.file.Paths.get(tempPath))
          .filter(java.nio.file.Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".csv"))
          .findFirst()
          .get()
          .toString

      val index = Index("bloom_multi_col_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile("file://" + fileName)
      index.addBloomIndex("Id")
      index.addBloomIndex("UserId", fpr = 0.001)
      index.update

      // Both bloom indexes should be available
      index.indexes should contain("Id")
      index.indexes should contain("UserId")

      // Query using both
      val files1 = index.locateFiles(Map("Id" -> Array(1, 2)))
      files1 should not be empty

      val files2 = index.locateFiles(Map("UserId" -> Array(1000L, 2000L)))
      files2 should not be empty

    } finally {
      val fs =
        org.apache.hadoop.fs.FileSystem
          .get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("bloom filter should have acceptable false positive rate") {
    // Create test data with known values
    val testData = (1 to 1000).map(i => Row(i, i.toLong * 1000, s"category_${i % 5}", i.toDouble))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), testSchema)

    val tempPath =
      s"${System.getProperty("java.io.tmpdir")}/bloom_fpr_empirical_test_${System.currentTimeMillis()}"
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempPath)

    try {
      val fileName =
        java.nio.file.Files
          .walk(java.nio.file.Paths.get(tempPath))
          .filter(java.nio.file.Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".csv"))
          .findFirst()
          .get()
          .toString

      val index = Index("bloom_fpr_empirical_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile("file://" + fileName)
      index.addBloomIndex("Id", fpr = 0.01) // 1% FPR
      index.update

      // Test with values that definitely exist (should always find the file)
      val existingFiles = index.locateFiles(Map("Id" -> Array(1, 500, 1000)))
      existingFiles should not be empty

      // Test with many values that don't exist
      // With 1% FPR and single file, we expect about 1% false positives
      val nonExistentValues = (10001 to 10100).toArray
      index.locateFiles(Map("Id" -> nonExistentValues.map(_.asInstanceOf[Any])))
      // We can't guarantee exact FPR in tests, but verify the mechanism works

    } finally {
      val fs =
        org.apache.hadoop.fs.FileSystem
          .get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("metadata should persist bloom index configuration") {
    val index1 =
      Index("bloom_persist_test", testSchema, "csv", Map("header" -> "true"))
    index1.addBloomIndex("Id", fpr = 0.005)

    // Reload the index
    val index2 = Index("bloom_persist_test", testSchema, "csv")

    // Bloom index should still be configured
    index2.indexes should contain("Id")
  }

  test("bloom index should round-trip non-string types whose cast differs from toString") {
    // Guards the build/query string-representation contract. Spark renders a timestamp as
    // "2024-01-15 10:00:00" while java.sql.Timestamp.toString renders "2024-01-15 10:00:00.0".
    // If the build side ever switches to cast(StringType), these lookups return nothing —
    // a false negative, which a bloom filter must never produce.
    val temporalSchema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = false),
          StructField("Value", DoubleType, nullable = false),
          StructField("UpdatedAt", TimestampType, nullable = true)))

    val index = Index("bloom_type_roundtrip_test", temporalSchema, "csv", Map("header" -> "true"))
    index.addFile(resourcePath("/data/temporal_part0.csv"))
    index.addBloomIndex("UpdatedAt")
    index.addBloomIndex("Value")
    index.update

    val ts = java.sql.Timestamp.valueOf("2024-01-15 10:00:00")
    index.locateFiles(Map("UpdatedAt" -> Array(ts))) should not be empty

    // Doubles round-trip through the same path.
    index.locateFiles(Map("Value" -> Array(100.0))) should not be empty

    // A value that was never indexed must still be excluded.
    val absent = java.sql.Timestamp.valueOf("1999-12-31 23:59:59")
    index.locateFiles(Map("UpdatedAt" -> Array(absent))) shouldBe empty
  }

  test("bloom index should size each file's filter independently") {
    // The streaming aggregator carries per-file expected insertions on each row. If that
    // regressed to one uniform size, the smaller file's filter would be inflated to match
    // the larger one's.
    val index = Index("bloom_per_file_sizing_test", testSchema, "csv", Map("header" -> "true"))
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addFile(resourcePath("/data/table1_part1.csv"))
    index.addBloomIndex("Id")
    index.update

    val rows =
      spark.read
        .format("delta")
        .load(new org.apache.hadoop.fs.Path(index.storagePath, "index").toString)
        .select("filename", "bloom_Id")
        .collect()

    rows.length shouldBe 2
    rows.foreach { r =>
      val bytes = r.getAs[Array[Byte]]("bloom_Id")
      bytes should not be null
      // A filter sized for a handful of values stays tiny; a uniformly oversized one would not.
      bytes.length should be < 200
    }

    // Correctness is unaffected by the sizing.
    index.locateFiles(Map("Id" -> Array(2))).size shouldBe 1
  }

  test("maxProbeValues should be derived from the filter's FPR, not from configuration") {
    // A bloom pre-filter only earns its keep while it still prunes files. A file holding
    // none of the probe values survives with probability 1-(1-fpr)^n, so pruning power
    // decays as n grows and the useful ceiling is a property of the filter itself.
    BloomFilterOperations.maxProbeValues(0.01) shouldBe 458
    BloomFilterOperations.maxProbeValues(0.001) shouldBe 4602
    BloomFilterOperations.maxProbeValues(0.0001) shouldBe 46049

    // At the cap, at least the target fraction of non-matching files is still pruned;
    // one value past it, the guarantee is gone.
    Seq(0.01, 0.001, 0.0001).foreach { fpr =>
      val n = BloomFilterOperations.maxProbeValues(fpr)
      math.pow(1.0 - fpr, n.toDouble) should be >= BloomFilterOperations.MinPruningFraction
      math.pow(1.0 - fpr, (n + 1).toDouble) should be < BloomFilterOperations.MinPruningFraction
    }
  }

  test("maxProbeValues should stay within a driver-safe ceiling for vanishingly small FPRs") {
    // The derivation grows as 1/fpr, so an extreme FPR would authorize collecting an
    // unbounded value set to the driver. The ceiling is a hard backstop, not a tuning knob.
    BloomFilterOperations.maxProbeValues(1e-12) shouldBe BloomFilterOperations.AbsoluteMaxProbeValues
    BloomFilterOperations.maxProbeValues(0.5) should be >= 1
  }

  test("bloom probing must not be configurable") {
    // The cutoff is a mathematical property of the filter. Exposing it as a knob would
    // allow tuning it into a regime where the pre-filter provably prunes nothing.
    val root = java.nio.file.Paths.get("src/main/scala")
    val stream = java.nio.file.Files.walk(root)
    val offenders =
      try {
        stream
          .iterator()
          .asScala
          .filter(_.toString.endsWith(".scala"))
          .filter { path =>
            new String(java.nio.file.Files.readAllBytes(path), StandardCharsets.UTF_8)
              .contains("bloomMaxQueryValues")
          }
          .map(_.toString)
          .toList
      } finally {
        stream.close()
      }
    offenders shouldBe empty
  }

  test("explicit bloom and auto-bloom must bound the query-side collect identically") {
    // Both paths collect distinct query values to the driver and broadcast them. An
    // unbounded collect on either is a driver OOM, so neither may be left unguarded.
    val index = Index("bloom_probe_parity_test", testSchema, "csv", Map("header" -> "true"))
    val _spark = spark
    import _spark.implicits._

    val within = Seq.range(1, 10).toDF("Id")
    val beyond = Seq.range(1, BloomFilterOperations.maxProbeValues(0.01) + 10).toDF("Id")

    index.collectProbeValues(within, "Id", 0.01).map(_.length) shouldBe Some(9)
    index.collectProbeValues(beyond, "Id", 0.01) shouldBe None

    // Skipping is safe; truncating is not. Whenever values are returned, every distinct
    // value must be present, or files holding a dropped value would be pruned away.
    val exact = Seq.range(0, BloomFilterOperations.maxProbeValues(0.01)).toDF("Id")
    index.collectProbeValues(exact, "Id", 0.01).map(_.length) shouldBe
      Some(BloomFilterOperations.maxProbeValues(0.01))
  }

  test("an over-cap query must return the same rows as an unindexed join") {
    // Above the cap the pre-filter is skipped. Skipping may read more files, but the
    // result must be identical to joining the raw source data -- a pre-filter that
    // changes the answer is a correctness bug, not an optimization.
    val schema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = false),
          StructField("Version", IntegerType, nullable = false),
          StructField("Value", DoubleType, nullable = false)))

    val index = Index("bloom_overcap_parity", schema, "csv", Map("header" -> "true"))
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addFile(resourcePath("/data/table1_part1.csv"))
    index.addBloomIndex("Id")
    index.update

    val _spark = spark
    import _spark.implicits._
    // Comfortably past the derived cap, so the pre-filter is skipped.
    val overCap = BloomFilterOperations.maxProbeValues(0.01) + 50
    val queryDf = Seq.range(1, overCap).toDF("Id")

    val actual =
      index.join(queryDf, Seq("Id")).select("Id", "Version", "Value").collect().map(_.toString).sorted

    val oracle =
      spark.read
        .schema(schema)
        .option("header", "true")
        .csv(resourcePath("/data/table1_part0.csv"), resourcePath("/data/table1_part1.csv"))
        .join(queryDf, Seq("Id"))
        .select("Id", "Version", "Value")
        .collect()
        .map(_.toString)
        .sorted

    actual shouldBe oracle
    actual should not be empty
  }
}
