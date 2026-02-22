package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import dev.cjfravel.ariadne.Index.DataFrameOps

class TemporalIndexTests extends SparkTests with Matchers {

  // Schema with Id, Value, and UpdatedAt timestamp
  // temporal_part0.csv: Id=1,2,3,4 with timestamps at 2024-01-15
  // temporal_part1.csv: Id=1,2,5 with timestamps at 2024-06 (1), 2024-03 (2), 2024-06 (5)
  // So latest for Id=1 is in part1 (2024-06), Id=2 in part1 (2024-03), Id=3 in part0, Id=4 in part0, Id=5 in part1
  val temporalSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false),
      StructField("UpdatedAt", TimestampType, nullable = true)
    )
  )

  val simpleSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("addTemporalIndex should add a temporal index configuration") {
    val index = Index("temporal_add_test", temporalSchema, "csv", Map("header" -> "true"))

    index.addTemporalIndex("Id", "UpdatedAt")

    index.indexes should contain("Id")
  }

  test("addTemporalIndex should be idempotent") {
    val index = Index("temporal_idempotent_test", temporalSchema, "csv", Map("header" -> "true"))

    index.addTemporalIndex("Id", "UpdatedAt")
    index.addTemporalIndex("Id", "UpdatedAt") // Should not throw

    index.indexes.count(_ == "Id") should be(1)
  }

  test("addTemporalIndex should reject non-existent value column") {
    val index = Index("temporal_bad_col_test", temporalSchema, "csv", Map("header" -> "true"))

    a[ColumnNotFoundException] should be thrownBy {
      index.addTemporalIndex("NonExistent", "UpdatedAt")
    }
  }

  test("addTemporalIndex should reject non-existent timestamp column") {
    val index = Index("temporal_bad_ts_test", temporalSchema, "csv", Map("header" -> "true"))

    a[ColumnNotFoundException] should be thrownBy {
      index.addTemporalIndex("Id", "NonExistent")
    }
  }

  test("addTemporalIndex and addIndex should be mutually exclusive") {
    val index1 = Index("temporal_excl_regular1", temporalSchema, "csv", Map("header" -> "true"))
    index1.addIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index1.addTemporalIndex("Id", "UpdatedAt")
    }

    val index2 = Index("temporal_excl_regular2", temporalSchema, "csv", Map("header" -> "true"))
    index2.addTemporalIndex("Id", "UpdatedAt")
    an[IllegalArgumentException] should be thrownBy {
      index2.addIndex("Id")
    }
  }

  test("addTemporalIndex and addBloomIndex should be mutually exclusive") {
    val index1 = Index("temporal_excl_bloom1", temporalSchema, "csv", Map("header" -> "true"))
    index1.addBloomIndex("Id")
    an[IllegalArgumentException] should be thrownBy {
      index1.addTemporalIndex("Id", "UpdatedAt")
    }

    val index2 = Index("temporal_excl_bloom2", temporalSchema, "csv", Map("header" -> "true"))
    index2.addTemporalIndex("Id", "UpdatedAt")
    an[IllegalArgumentException] should be thrownBy {
      index2.addBloomIndex("Id")
    }
  }

  test("metadata should persist temporal index configuration") {
    val index1 = Index("temporal_persist_test", temporalSchema, "csv", Map("header" -> "true"))
    index1.addTemporalIndex("Id", "UpdatedAt")

    // Reload the index
    val index2 = Index("temporal_persist_test", temporalSchema, "csv")
    index2.indexes should contain("Id")
  }

  test("should build temporal index during update") {
    val index = Index("temporal_build_test", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath = resourcePath("/data/temporal_part0.csv")
    index.addFile(csvPath)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    index.indexes should contain("Id")
    index.unindexedFiles.size should be(0)
  }

  test("should locate files using temporal index with file pruning") {
    val index = Index("temporal_locate_test", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // Id=1 appears in both files but latest is in part1 (2024-06-15 > 2024-01-15)
    // File pruning should return only the file with the latest timestamp
    val files1 = index.locateFiles(Map("Id" -> Array(1)))
    files1.size should be(1) // Only the file with the latest version
    files1.head should include("temporal_part1.csv")

    // Id=3 only appears in part0 → returns part0
    val files3 = index.locateFiles(Map("Id" -> Array(3)))
    files3.size should be(1)
    files3.head should include("temporal_part0.csv")

    // Id=1 (latest in part1) and Id=3 (only in part0) → both files needed
    val filesBoth = index.locateFiles(Map("Id" -> Array(1, 3)))
    filesBoth.size should be(2)
  }

  test("temporal join should return only the latest version of each entity") {
    val index = Index("temporal_dedup_test", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // Query for Id=1 which appears in both files
    // part0: Id=1, Value=100.0, UpdatedAt=2024-01-15
    // part1: Id=1, Value=150.0, UpdatedAt=2024-06-15 (LATEST)
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    )

    val result = index.join(queryData, Seq("Id"), "inner")
    result.count() should be(1) // Only one row for Id=1
    result.select("Value").collect().head.getDouble(0) should be(150.0) // Latest value
  }

  test("temporal join should deduplicate across multiple entities") {
    val index = Index("temporal_multi_dedup_test", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // Query for Id=1 and Id=2, both in both files
    // Id=1: latest in part1 (Value=150.0)
    // Id=2: latest in part1 (Value=250.0)
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
      StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    )

    val result = index.join(queryData, Seq("Id"), "inner")
    result.count() should be(2) // One per Id

    val resultMap = result.select("Id", "Value").collect().map(r => r.getInt(0) -> r.getDouble(1)).toMap
    resultMap(1) should be(150.0) // Latest for Id=1
    resultMap(2) should be(250.0) // Latest for Id=2
  }

  test("temporal join should handle entities only in one file") {
    val index = Index("temporal_single_file_test", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // Query for Id=3 (only in part0) and Id=5 (only in part1)
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(3), Row(5))),
      StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    )

    val result = index.join(queryData, Seq("Id"), "inner")
    result.count() should be(2)

    val resultMap = result.select("Id", "Value").collect().map(r => r.getInt(0) -> r.getDouble(1)).toMap
    resultMap(3) should be(300.0) // Only version
    resultMap(5) should be(500.0) // Only version
  }

  test("temporal join with DataFrame.join implicit should work") {
    val index = Index("temporal_implicit_join_test", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
      StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    )

    val result = queryData.join(index, Seq("Id"), "inner")
    result.count() should be(2)
  }

  test("should support mixed temporal and regular indexes") {
    val mixedSchema = StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Category", StringType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true)
      )
    )

    // Create test data with categories and timestamps
    val testData1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "A", 100.0, java.sql.Timestamp.valueOf("2024-01-15 10:00:00")),
        Row(2, "B", 200.0, java.sql.Timestamp.valueOf("2024-01-15 10:00:00"))
      )),
      mixedSchema
    )
    val testData2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "A", 150.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")),
        Row(3, "C", 300.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00"))
      )),
      mixedSchema
    )

    val tempPath1 = s"${System.getProperty("java.io.tmpdir")}/temporal_mixed_1_${System.currentTimeMillis()}"
    val tempPath2 = s"${System.getProperty("java.io.tmpdir")}/temporal_mixed_2_${System.currentTimeMillis()}"
    testData1.coalesce(1).write.mode("overwrite").parquet(tempPath1)
    testData2.coalesce(1).write.mode("overwrite").parquet(tempPath2)

    try {
      val index = Index("temporal_mixed_test", mixedSchema, "parquet")
      index.addFile(tempPath1, tempPath2)
      index.addTemporalIndex("Id", "UpdatedAt") // Temporal index
      index.addIndex("Category") // Regular index
      index.update

      index.indexes should contain("Id")
      index.indexes should contain("Category")

      // Join on temporal column should dedup
      val queryData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false)))
      )
      val result = index.join(queryData, Seq("Id"), "inner")
      result.count() should be(1)
      result.select("Value").collect().head.getDouble(0) should be(150.0) // Latest
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath1), true)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath2), true)
    }
  }

  test("temporal join should handle null timestamps (nulls ranked last)") {
    val nullTsSchema = StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true)
      )
    )

    val testData1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 100.0, null)  // null timestamp
      )),
      nullTsSchema
    )
    val testData2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 150.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00"))
      )),
      nullTsSchema
    )

    val tempPath1 = s"${System.getProperty("java.io.tmpdir")}/temporal_null_1_${System.currentTimeMillis()}"
    val tempPath2 = s"${System.getProperty("java.io.tmpdir")}/temporal_null_2_${System.currentTimeMillis()}"
    testData1.coalesce(1).write.mode("overwrite").parquet(tempPath1)
    testData2.coalesce(1).write.mode("overwrite").parquet(tempPath2)

    try {
      val index = Index("temporal_null_ts_test", nullTsSchema, "parquet")
      index.addFile(tempPath1, tempPath2)
      index.addTemporalIndex("Id", "UpdatedAt")
      index.update

      val queryData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false)))
      )
      val result = index.join(queryData, Seq("Id"), "inner")
      result.count() should be(1)
      result.select("Value").collect().head.getDouble(0) should be(150.0) // Non-null ts wins
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath1), true)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath2), true)
    }
  }
}
