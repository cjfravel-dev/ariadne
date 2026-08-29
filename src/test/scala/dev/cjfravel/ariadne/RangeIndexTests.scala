package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests for range index support covering index creation, idempotency, min/max value tracking per file, and range-based
 * file location queries.
 */
class RangeIndexTests extends SparkTests with Matchers {

  // table1_part0.csv: Id=[1,2,3,1], Version=[1,1,1,2], Value=[5.0,3.0,4.0,4.5]
  //   -> Id range [1,3], Version range [1,2], Value range [3.0,5.0]
  // table1_part1.csv: Id=[4,4,3,1], Version=[1,2,2,3], Value=[2.0,9.0,4.0,5.0]
  //   -> Id range [1,4], Version range [1,3], Value range [2.0,9.0]
  val testSchema: StructType =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Version", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false)))

  test("should add range index") {
    val index =
      Index("range_add_test", testSchema, "csv", Map("header" -> "true"))
    index.addRangeIndex("Id")
    index.indexes should contain("Id")
  }

  test("should be idempotent") {
    val index =
      Index("range_idempotent_test", testSchema, "csv", Map("header" -> "true"))
    index.addRangeIndex("Id")
    index.addRangeIndex("Id")
    index.indexes.count(_ == "Id") should be(1)
  }

  test("should store min/max per file") {
    val index =
      Index("range_minmax_test", testSchema, "csv", Map("header" -> "true"))
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath0)
    index.addFile(csvPath1)
    index.addRangeIndex("Id")
    index.update

    // Read the index table directly
    val indexPath = new org.apache.hadoop.fs.Path(index.storagePath, "index")
    val df = spark.read.format("delta").load(indexPath.toString)
    df.columns should contain("range_Id")

    val rows = df.select("filename", "range_Id").collect()
    rows.length should be(2)

    rows.foreach { row =>
      val filename = row.getString(0)
      val rangeStruct = row.getStruct(1)
      val minVal = rangeStruct.getInt(0)
      val maxVal = rangeStruct.getInt(1)

      if (filename.contains("part0")) {
        minVal should be(1)
        maxVal should be(3)
      } else {
        minVal should be(1)
        maxVal should be(4)
      }
    }
  }

  test("should prune files by range") {
    val index =
      Index("range_prune_test", testSchema, "csv", Map("header" -> "true"))
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath0)
    index.addFile(csvPath1)
    index.addRangeIndex("Id")
    index.update

    // Id=4 only exists in part1 (range [1,4]), part0 range is [1,3]
    val files = index.locateFiles(Map("Id" -> Array(4)))
    files should have size 1
    files.head should include("part1")
  }

  test("should return all files when value in all ranges") {
    val index =
      Index("range_all_files_test", testSchema, "csv", Map("header" -> "true"))
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath0)
    index.addFile(csvPath1)
    index.addRangeIndex("Id")
    index.update

    // Id=1 is in range for both files
    val files = index.locateFiles(Map("Id" -> Array(1)))
    files should have size 2
  }

  test("should handle range query with DataFrame join") {
    val index =
      Index("range_join_test", testSchema, "csv", Map("header" -> "true"))
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath0)
    index.addFile(csvPath1)
    index.addRangeIndex("Id")
    index.update

    // Create a DataFrame with Id=4 which only appears in part1
    val queryDf =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(4))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    val result = index.join(queryDf, Seq("Id"))
    result.count() should be > 0L
    // Should only have rows from part1 (Id=4 data)
    result.select("Id").distinct().collect().map(_.getInt(0)) should contain(4)
  }

  test("should enforce mutual exclusivity with regular index") {
    val index =
      Index("range_excl_regular", testSchema, "csv", Map("header" -> "true"))
    index.addIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index.addRangeIndex("Id")
    }
  }

  test("should enforce mutual exclusivity with bloom index") {
    val index =
      Index("range_excl_bloom", testSchema, "csv", Map("header" -> "true"))
    index.addBloomIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index.addRangeIndex("Id")
    }
  }

  test("should enforce mutual exclusivity - range blocks regular") {
    val index =
      Index("range_blocks_regular", testSchema, "csv", Map("header" -> "true"))
    index.addRangeIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index.addIndex("Id")
    }
  }

  test("should enforce mutual exclusivity - range blocks bloom") {
    val index =
      Index("range_blocks_bloom", testSchema, "csv", Map("header" -> "true"))
    index.addRangeIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index.addBloomIndex("Id")
    }
  }

  test("should enforce mutual exclusivity - range blocks temporal") {
    val index =
      Index("range_blocks_temporal", testSchema, "csv", Map("header" -> "true"))
    index.addRangeIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index.addTemporalIndex("Id", "Version")
    }
  }

  test("should throw ColumnNotFoundException for nonexistent column") {
    val index =
      Index("range_bad_col", testSchema, "csv", Map("header" -> "true"))
    a[ColumnNotFoundException] should be thrownBy {
      index.addRangeIndex("NonExistent")
    }
  }

  test("should handle metadata migration from v6") {
    val stream = getClass.getResourceAsStream("/index_metadata/v6.json")
    require(stream != null, "Resource not found: /index_metadata/v6.json")
    val jsonString =
      new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    // v6 -> v7 migration should add empty range_indexes
    metadata.range_indexes should not be null
    metadata.range_indexes.size() should be(0)
  }

  test("should parse v7 metadata with range indexes") {
    val stream = getClass.getResourceAsStream("/index_metadata/v7.json")
    require(stream != null, "Resource not found: /index_metadata/v7.json")
    val jsonString =
      new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    metadata.range_indexes.size() should be(2)

    import scala.collection.JavaConverters._
    val rangeIndexes = metadata.range_indexes.asScala.toSeq
    rangeIndexes.map(_.column) should contain allOf ("amount", "score")
  }

  test("should backfill range index on existing files") {
    val index =
      Index("range_backfill_test", testSchema, "csv", Map("header" -> "true"))
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath0)
    index.addFile(csvPath1)
    index.addIndex("Id")
    index.update

    // Now add range index and update again
    index.addRangeIndex("Version")
    index.update

    val indexPath = new org.apache.hadoop.fs.Path(index.storagePath, "index")
    val df = spark.read.format("delta").load(indexPath.toString)
    df.columns should contain("range_Version")

    // Verify range values
    val rows = df.select("filename", "range_Version").collect()
    rows.foreach { row =>
      val filename = row.getString(0)
      val rangeStruct = row.getStruct(1)
      val minVal = rangeStruct.getInt(0)
      val maxVal = rangeStruct.getInt(1)

      if (filename.contains("part0")) {
        minVal should be(1)
        maxVal should be(2)
      } else {
        minVal should be(1)
        maxVal should be(3)
      }
    }
  }

  /**
   * Counts Spark jobs submitted while `body` runs.
   *
   * Job count is the only externally observable measure of how many actions a query performs, so it is asserted
   * directly rather than inferred from timing.
   */
  private def countingJobs[A](body: => A): (A, Int) = {
    val jobs = new java.util.concurrent.atomic.AtomicInteger(0)
    val listener =
      new org.apache.spark.scheduler.SparkListener {
        override def onJobStart(e: org.apache.spark.scheduler.SparkListenerJobStart): Unit = {
          jobs.incrementAndGet()
          ()
        }
      }
    spark.sparkContext.addSparkListener(listener)
    try {
      val result = body
      // Job-start events are posted asynchronously, so settle before reading the counter:
      // wait until the count stops moving rather than guessing a fixed sleep.
      var stable = 0
      var last = -1
      val deadline = System.currentTimeMillis() + 10000
      while (stable < 3 && System.currentTimeMillis() < deadline) {
        Thread.sleep(150)
        val current = jobs.get()
        if (current == last) stable += 1 else stable = 0
        last = current
      }
      (result, jobs.get())
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  private def rangeIndexOver(name: String): Index = {
    val index = Index(name, testSchema, "csv", Map("header" -> "true"))
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addFile(resourcePath("/data/table1_part1.csv"))
    index.addRangeIndex("Id")
    index.update
    index
  }

  private def unindexedOracle(queryDf: org.apache.spark.sql.DataFrame): Set[String] =
    spark.read
      .schema(testSchema)
      .option("header", "true")
      .csv(resourcePath("/data/table1_part0.csv"), resourcePath("/data/table1_part1.csv"))
      .join(queryDf, Seq("Id"))
      .select("Id", "Version", "Value")
      .collect()
      .map(_.toString)
      .toSet

  test("range query must not submit a separate counting job to choose its strategy") {
    val index = rangeIndexOver("range_single_job_test")
    val _spark = spark
    import _spark.implicits._

    // Past the per-value threshold, so the bounding-box strategy is selected.
    val queryDf = Seq.range(1, 1500).toDF("Id").cache()
    queryDf.count() // materialize the cache outside the measured region

    val (files, jobs) =
      countingJobs(index.locateFilesFromDataFrame(queryDf, Map("Id" -> "Id"), Seq("Id")))

    // Choosing between the per-value and bounding-box strategies must not cost an action of
    // its own: the collect that supplies the values also decides the branch. Measured at 10
    // jobs for this query on Spark 3.5, against 12 when the branch is chosen by a separate
    // count. The bound carries one job of headroom, since planning of limit and aggregate
    // differs slightly across Spark lines, and still fails on the extra count.
    withClue(s"jobs=$jobs for ${files.size} file(s): ") {
      jobs should be <= 11
    }
    queryDf.unpersist()
  }

  test("range query results must match an unindexed join on both sides of the strategy threshold") {
    val index = rangeIndexOver("range_threshold_parity_test")
    val _spark = spark
    import _spark.implicits._

    // 999 and 1000 take per-value pruning; 1001 and beyond switch to the bounding box.
    // The strategy is an optimization, so every one of them must agree with the oracle.
    Seq(999, 1000, 1001, 1500).foreach { n =>
      val queryDf = Seq.range(1, n + 1).toDF("Id")
      val actual =
        index.join(queryDf, Seq("Id")).select("Id", "Version", "Value").collect().map(_.toString).toSet
      withClue(s"query with $n distinct values: ") {
        actual shouldBe unindexedOracle(queryDf)
        actual should not be empty
      }
    }
  }

  test("a query value set far beyond any internal bound must still find every matching file") {
    // Query value sets are unbounded, while any internal collect is bounded. A bound that
    // ever narrowed the candidate files would prune away files holding the excluded values,
    // so assert the files directly rather than relying on the bound's placement.
    val index = rangeIndexOver("range_beyond_bound_test")
    val _spark = spark
    import _spark.implicits._

    val queryDf = Seq.range(1, 20000).toDF("Id")
    val files = index.locateFilesFromDataFrame(queryDf, Map("Id" -> "Id"), Seq("Id"))

    // Every indexed file holds Ids inside [1, 20000), so none may be pruned.
    files.size shouldBe 2

    val actual = index.join(queryDf, Seq("Id")).select("Id", "Version", "Value").collect().map(_.toString).toSet
    actual shouldBe unindexedOracle(queryDf)
  }

  test("per-value range pruning must plan a value set at the strategy threshold") {
    // The per-value strategy builds one containment predicate per value and combines them.
    // Combined as a chain, the predicate tree is as deep as the value set, and Delta's data
    // skipping recurses over it until the stack gives out -- inside the size this strategy
    // accepts. Both entry points build the predicate, so both are exercised.
    val index = rangeIndexOver("range_predicate_depth_test")
    val _spark = spark
    import _spark.implicits._

    val values = Array.range(1, 1001)
    val fromArray = index.locateFiles(Map("Id" -> values.map(_.asInstanceOf[Any])))
    fromArray.size shouldBe 2

    val queryDf = values.toSeq.toDF("Id")
    val fromDf = index.locateFilesFromDataFrame(queryDf, Map("Id" -> "Id"), Seq("Id"))
    fromDf.size shouldBe 2
  }
}
