package dev.cjfravel.ariadne
import dev.cjfravel.ariadne.Index.DataFrameOps
import dev.cjfravel.ariadne.exceptions.UnsupportedJoinTypeException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[IndexJoinOperations]] verifying index-accelerated joins between a DataFrame and indexed data, including
 * both `index.join(df)` and `df.join(index)` directions.
 */
class IndexJoinOperationsTests extends SparkTests with Matchers {

  val indexSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Version", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false)))

  val querySchema =
    StructType(
      Seq(StructField("Id", IntegerType, nullable = false), StructField("Version", IntegerType, nullable = false)))

  val explodedSchema =
    StructType(
      Seq(
        StructField("event_id", StringType, nullable = true),
        StructField(
          "users",
          ArrayType(StructType(
            Seq(StructField("id", LongType, nullable = true), StructField("name", StringType, nullable = true)))),
          nullable = true)))

  test("should perform basic inner join correctly") {
    val index =
      Index("join_basic_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    // Create query DataFrame
    val queryData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 2))), querySchema)

    // Test DataFrame.join(index, ...)
    val result1 = queryData.join(index, Seq("Id", "Version"), "inner")
    result1.count() should be > 0L
    result1.columns should contain allOf ("Id", "Version", "Value")

    // Test index.join(DataFrame, ...)
    val result2 = index.join(queryData, Seq("Id", "Version"), "inner")
    result2.count() should be > 0L
    result2.columns should contain allOf ("Id", "Version", "Value")
  }

  test("should perform left semi join correctly") {
    val index =
      Index("join_semi_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(1, 1),
            Row(2, 2),
            Row(999, 999) // This ID shouldn't exist in the index
          )),
        querySchema)

    val result = queryData.join(index, Seq("Id"), "left_semi")
    result.count() should be < queryData.count()
    result.columns should not contain "Value" // Semi join should only return left side columns
  }

  test("should preserve unmatched query rows with a right join") {
    val index =
      Index("join_outer_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(1, 1),
            Row(999, 999) // This ID shouldn't exist in the index
          )),
        querySchema)

    // The index is on the left, so "right" preserves every query row including the unmatched one.
    val result = index.join(queryData, Seq("Id"), "right")
    result.count() should be >= queryData.count()
    result.columns should contain allOf ("Id", "Version", "Value")

    // "fullouter" would additionally require index rows with no query match. Those are pruned at file granularity,
    // so the answer would depend on file layout rather than on the data.
    a[UnsupportedJoinTypeException] should be thrownBy index.join(queryData, Seq("Id"), "fullouter")
  }

  test("should reject join types defined by unmatched index rows") {
    val index =
      Index("join_reject_test", indexSchema, "csv", Map("header" -> "true"))

    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addIndex("Id")
    index.update

    val queryData =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 1))), querySchema)

    // index.join puts the index on the left, so left-preserving types are rejected in every alias spelling.
    Seq(
      "left",
      "left_outer",
      "leftouter",
      "LEFT_OUTER",
      "left_anti",
      "leftanti",
      "anti",
      "full",
      "fullouter",
      "full_outer",
      "outer").foreach { joinType =>
      withClue(s"index.join should reject '$joinType': ") {
        a[UnsupportedJoinTypeException] should be thrownBy index.join(queryData, Seq("Id"), joinType)
      }
    }

    // df.join(index) puts the index on the right, so right-preserving types are rejected instead.
    Seq("right", "right_outer", "rightouter", "RIGHT", "full", "fullouter", "full_outer", "outer").foreach { joinType =>
      withClue(s"df.join(index) should reject '$joinType': ") {
        a[UnsupportedJoinTypeException] should be thrownBy queryData.join(index, Seq("Id"), joinType)
      }
    }

    // The mirror-image types stay available on each side.
    Seq("inner", "left_semi", "right").foreach(joinType => index.join(queryData, Seq("Id"), joinType).count())
    Seq("inner", "left_semi", "left", "left_anti").foreach { joinType =>
      queryData.join(index, Seq("Id"), joinType).count()
    }
  }

  test("cross join type is accepted and degenerates to inner") {
    val index =
      Index("join_cross_test", indexSchema, "csv", Map("header" -> "true"))

    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addIndex("Id")
    index.update

    val queryData =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 1))), querySchema)

    // Both entry points always supply join columns, so Spark attaches an equi-join condition
    // and `cross` produces exactly the inner-join result. Pinned because the scaladoc says so.
    val collectRows = (df: org.apache.spark.sql.DataFrame) => df.collect().map(_.toString).sorted.toSeq

    collectRows(index.join(queryData, Seq("Id"), "cross")) should
      contain theSameElementsAs collectRows(index.join(queryData, Seq("Id"), "inner"))

    collectRows(queryData.join(index, Seq("Id"), "cross")) should
      contain theSameElementsAs collectRows(queryData.join(index, Seq("Id"), "inner"))
  }

  test("should handle joins with single column") {
    val index =
      Index("join_single_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Version")
    index.update

    val singleColumnSchema =
      StructType(Seq(StructField("Version", IntegerType, nullable = false)))
    val queryData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1), Row(2))), singleColumnSchema)

    val result = queryData.join(index, Seq("Version"), "inner")
    result.count() should be > 0L
    result.columns should contain allOf ("Version", "Id", "Value")
  }

  test("should handle joins with exploded field indexes") {
    // Create test data with arrays
    val testData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row("evt1", Array(Row(100L, "Alice"), Row(101L, "Bob"))),
            Row("evt2", Array(Row(102L, "Charlie"))),
            Row("evt3", Array(Row(100L, "Alice"), Row(103L, "David"))))),
        explodedSchema)

    val tempPath =
      s"${System.getProperty("java.io.tmpdir")}/join_exploded_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)

    try {
      val readOptions = Map("multiLine" -> "true")
      val index =
        Index("join_exploded_test", explodedSchema, "json", readOptions)

      index.addFile(tempPath)
      index.addExplodedFieldIndex("users", "id", "user_id")
      index.update

      // Create query data using the exploded field column name
      val userQuerySchema =
        StructType(Seq(StructField("user_id", LongType, nullable = false)))
      val queryData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(100L), Row(101L))), userQuerySchema)

      val result = queryData.join(index, Seq("user_id"), "inner")
      result.count() should be > 0L
      result.columns should contain allOf ("user_id", "event_id")

    } finally {
      val fs =
        org.apache.hadoop.fs.FileSystem
          .get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle joins with computed indexes") {
    val index =
      Index("join_computed_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addComputedIndex("id_doubled", "Id * 2")
    index.update

    val computedSchema =
      StructType(Seq(StructField("id_doubled", IntegerType, nullable = false)))
    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(2), // 2 * 1
            Row(4), // 2 * 2
            Row(6) // 2 * 3
          )),
        computedSchema)

    val result = queryData.join(index, Seq("id_doubled"), "inner")
    result.count() should be > 0L
    result.columns should contain allOf ("id_doubled", "Id", "Version", "Value")
  }

  test("should handle joins with mixed index types") {
    val mixedSchema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = true),
          StructField("priority", IntegerType, nullable = true),
          StructField(
            "users",
            ArrayType(StructType(
              Seq(StructField("id", LongType, nullable = true), StructField("role", StringType, nullable = true)))),
            nullable = true)))

    val testData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row("evt1", 1, Array(Row(100L, "admin"))),
            Row("evt2", 2, Array(Row(101L, "user"))),
            Row("evt3", 3, Array(Row(102L, "admin"))))),
        mixedSchema)

    val tempPath =
      s"${System.getProperty("java.io.tmpdir")}/join_mixed_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)

    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("join_mixed_test", mixedSchema, "json", readOptions)

      index.addFile(tempPath)
      index.addIndex("event_id") // Regular index
      index.addComputedIndex("priority_level", "case when priority > 2 then 'high' else 'low' end") // Computed
      index.addExplodedFieldIndex("users", "id", "user_id") // Exploded field
      index.update

      // Test join with all three index types
      val multiQuerySchema =
        StructType(
          Seq(
            StructField("event_id", StringType, nullable = false),
            StructField("priority_level", StringType, nullable = false),
            StructField("user_id", LongType, nullable = false)))

      val queryData =
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row("evt1", "low", 100L), Row("evt3", "high", 102L))),
          multiQuerySchema)

      val result = queryData.join(index, Seq("event_id", "priority_level", "user_id"), "inner")
      result.count() should be > 0L
      result.columns should contain allOf ("event_id", "priority_level", "user_id", "priority")

    } finally {
      val fs =
        org.apache.hadoop.fs.FileSystem
          .get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle joins with no matching data") {
    val index =
      Index("join_no_match_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    val queryData =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(999, 999), Row(998, 998))), querySchema)

    // Inner join should return no results
    val innerResult = queryData.join(index, Seq("Id"), "inner")
    innerResult.count() should be(0)

    // Left join should return query data with nulls
    val leftResult = queryData.join(index, Seq("Id"), "left")
    leftResult.count() should be(queryData.count())
  }

  test("should cache join results for performance") {
    val index =
      Index("join_cache_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    val queryData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 2))), querySchema)

    // First join - should populate cache
    val result1 = queryData.join(index, Seq("Id"), "inner")
    val count1 = result1.count()

    // Second join with same parameters - should use cache
    val result2 = queryData.join(index, Seq("Id"), "inner")
    val count2 = result2.count()

    count1 should equal(count2)
    count1 should be > 0L
  }

  test("should handle joins with repartition option set") {
    spark.conf.set("spark.ariadne.indexRepartitionCount", "4")
    try {
      val index = Index("join_repartition_test", indexSchema, "csv", Map("header" -> "true"))

      val csvPath = resourcePath("/data/table1_part0.csv")
      index.addFile(csvPath)
      index.addIndex("Id")
      index.addIndex("Version")
      index.update

      val queryData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 2))), querySchema)

      // Test DataFrame.join(index, ...) with repartitioning enabled
      val result1 = queryData.join(index, Seq("Id", "Version"), "inner")
      result1.count() should be > 0L
      result1.columns should contain allOf ("Id", "Version", "Value")

      // Test index.join(DataFrame, ...) with repartitioning enabled
      val result2 = index.join(queryData, Seq("Id", "Version"), "inner")
      result2.count() should be > 0L
      result2.columns should contain allOf ("Id", "Version", "Value")

      // Results should match regardless of join direction
      result1.count() should equal(result2.count())
    } finally {
      spark.conf.unset("spark.ariadne.indexRepartitionCount")
    }
  }

  test("should produce same results with and without repartitioning") {
    val index = Index("join_repartition_compare_test", indexSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath0)
    index.addFile(csvPath1)
    index.addIndex("Id")
    index.update

    val queryData =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 1), Row(2, 2), Row(3, 3))), querySchema)

    // Join without repartitioning
    val resultWithout = queryData.join(index, Seq("Id"), "inner")
    val countWithout = resultWithout.count()

    // Join with repartitioning
    spark.conf.set("spark.ariadne.indexRepartitionCount", "2")
    try {
      val resultWith = queryData.join(index, Seq("Id"), "inner")
      val countWith = resultWith.count()

      countWith should equal(countWithout)
      countWith should be > 0L
    } finally {
      spark.conf.unset("spark.ariadne.indexRepartitionCount")
    }
  }

  test("join-type validation rejects an unrecognized index side instead of guessing") {
    // Both helpers branch on indexSide; defaulting an unknown value to one side would silently apply the wrong
    // rejection set and name the wrong alternatives in the resulting error message.
    val validateFailure =
      intercept[IllegalArgumentException] {
        IndexJoinOperations.validateJoinType("inner", "middle")
      }
    validateFailure.getMessage should include("middle")

    val supportedFailure =
      intercept[IllegalArgumentException] {
        IndexJoinOperations.supportedFor("neither")
      }
    supportedFailure.getMessage should include("neither")

    IndexJoinOperations.supportedFor("left") should contain("right_outer")
    IndexJoinOperations.supportedFor("right") should contain("left_outer")
  }
}
