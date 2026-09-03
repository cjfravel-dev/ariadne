package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.Index.DataFrameOps
import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests for temporal index support covering index creation, idempotency, validation of key/value columns, and temporal
 * deduplication during joins.
 */
class TemporalIndexTests extends SparkTests with Matchers {

  // Schema with Id, Value, and UpdatedAt timestamp
  // temporal_part0.csv: Id=1,2,3,4 with timestamps at 2024-01-15
  // temporal_part1.csv: Id=1,2,5 with timestamps at 2024-06 (1), 2024-03 (2), 2024-06 (5)
  // So latest for Id=1 is in part1 (2024-06), Id=2 in part1 (2024-03), Id=3 in part0, Id=4 in part0, Id=5 in part1
  val temporalSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true)))

  val simpleSchema =
    StructType(
      Seq(StructField("Id", IntegerType, nullable = false), StructField("Value", DoubleType, nullable = false)))

  test("addTemporalIndex should add a temporal index configuration") {
    val index =
      Index("temporal_add_test", temporalSchema, "csv", Map("header" -> "true"))

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
    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

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
    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    val result = index.join(queryData, Seq("Id"), "inner")
    result.count() should be(2) // One per Id

    val resultMap =
      result
        .select("Id", "Value")
        .collect()
        .map(r => r.getInt(0) -> r.getDouble(1))
        .toMap
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
    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(3), Row(5))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    val result = index.join(queryData, Seq("Id"), "inner")
    result.count() should be(2)

    val resultMap =
      result
        .select("Id", "Value")
        .collect()
        .map(r => r.getInt(0) -> r.getDouble(1))
        .toMap
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

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    val result = queryData.join(index, Seq("Id"), "inner")
    result.count() should be(2)
  }

  test("should support mixed temporal and regular indexes") {
    val mixedSchema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = false),
          StructField("Category", StringType, nullable = false),
          StructField("Value", DoubleType, nullable = false),
          StructField("UpdatedAt", TimestampType, nullable = true)))

    // Create test data with categories and timestamps
    val testData1 =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(1, "A", 100.0, java.sql.Timestamp.valueOf("2024-01-15 10:00:00")),
            Row(2, "B", 200.0, java.sql.Timestamp.valueOf("2024-01-15 10:00:00")))),
        mixedSchema)
    val testData2 =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(1, "A", 150.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")),
            Row(3, "C", 300.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")))),
        mixedSchema)

    val tempPath1 =
      s"${System.getProperty("java.io.tmpdir")}/temporal_mixed_1_${System.currentTimeMillis()}"
    val tempPath2 =
      s"${System.getProperty("java.io.tmpdir")}/temporal_mixed_2_${System.currentTimeMillis()}"
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
      val queryData =
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1))),
          StructType(Seq(StructField("Id", IntegerType, nullable = false))))
      val result = index.join(queryData, Seq("Id"), "inner")
      result.count() should be(1)
      result.select("Value").collect().head.getDouble(0) should be(150.0) // Latest
    } finally {
      val fs =
        org.apache.hadoop.fs.FileSystem
          .get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath1), true)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath2), true)
    }
  }

  test("temporal join should handle null timestamps (nulls ranked last)") {
    val nullTsSchema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = false),
          StructField("Value", DoubleType, nullable = false),
          StructField("UpdatedAt", TimestampType, nullable = true)))

    val testData1 =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(1, 100.0, null) // null timestamp
          )),
        nullTsSchema)
    val testData2 =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 150.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")))),
        nullTsSchema)

    val tempPath1 =
      s"${System.getProperty("java.io.tmpdir")}/temporal_null_1_${System.currentTimeMillis()}"
    val tempPath2 =
      s"${System.getProperty("java.io.tmpdir")}/temporal_null_2_${System.currentTimeMillis()}"
    testData1.coalesce(1).write.mode("overwrite").parquet(tempPath1)
    testData2.coalesce(1).write.mode("overwrite").parquet(tempPath2)

    try {
      val index = Index("temporal_null_ts_test", nullTsSchema, "parquet")
      index.addFile(tempPath1, tempPath2)
      index.addTemporalIndex("Id", "UpdatedAt")
      index.update

      val queryData =
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1))),
          StructType(Seq(StructField("Id", IntegerType, nullable = false))))
      val result = index.join(queryData, Seq("Id"), "inner")
      result.count() should be(1)
      result.select("Value").collect().head.getDouble(0) should be(150.0) // Non-null ts wins
    } finally {
      val fs =
        org.apache.hadoop.fs.FileSystem
          .get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath1), true)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath2), true)
    }
  }

  test("multiple temporal indexes should rank each column against the original rows") {
    val multiTemporalSchema =
      StructType(
        Seq(
          StructField("EntityA", IntegerType, nullable = false),
          StructField("UpdatedA", TimestampType, nullable = false),
          StructField("EntityB", StringType, nullable = false),
          StructField("UpdatedB", TimestampType, nullable = false),
          StructField("Payload", StringType, nullable = false)))

    val index = Index("multi_temporal_independent_ranks", multiTemporalSchema, "parquet")
    index.addTemporalIndex("EntityA", "UpdatedA")
    index.addTemporalIndex("EntityB", "UpdatedB")

    val rows =
      Seq(
        Row(
          1,
          java.sql.Timestamp.valueOf("2024-03-01 00:00:00"),
          "x",
          java.sql.Timestamp.valueOf("2024-01-01 00:00:00"),
          "stale-for-b"),
        Row(
          2,
          java.sql.Timestamp.valueOf("2024-01-01 00:00:00"),
          "x",
          java.sql.Timestamp.valueOf("2024-03-01 00:00:00"),
          "stale-for-a"),
        Row(
          2,
          java.sql.Timestamp.valueOf("2024-02-01 00:00:00"),
          "y",
          java.sql.Timestamp.valueOf("2024-02-01 00:00:00"),
          "latest-for-both"))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), multiTemporalSchema)

    val result = index.applyTemporalDeduplication(df, Seq("EntityA", "EntityB"))

    result.select("Payload").collect().map(_.getString(0)).toSeq shouldBe Seq("latest-for-both")
  }

  test("temporal join should deduplicate when select() drops the timestamp column") {
    val index = Index("temporal_select_drops_timestamp", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    // UpdatedAt is required for deduplication but is not selected: it should be
    // read transparently and dropped from the result.
    val result = index.select("Id", "Value").join(queryData, Seq("Id"), "inner")

    result.columns should contain theSameElementsAs Seq("Id", "Value")
    result.count() should be(1)
    result.select("Value").collect().head.getDouble(0) should be(150.0)
  }

  test("temporal join should keep the timestamp column when it is explicitly selected") {
    val index = Index("temporal_select_keeps_timestamp", temporalSchema, "csv", Map("header" -> "true"))

    val csvPath0 = resourcePath("/data/temporal_part0.csv")
    val csvPath1 = resourcePath("/data/temporal_part1.csv")
    index.addFile(csvPath0, csvPath1)
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    val result = index.select("Id", "Value", "UpdatedAt").join(queryData, Seq("Id"), "inner")

    result.columns should contain theSameElementsAs Seq("Id", "Value", "UpdatedAt")
    result.count() should be(1)
    result.select("Value").collect().head.getDouble(0) should be(150.0)
  }

  // Schema whose timestamp lives inside a struct, so the temporal timestamp is the nested path
  // `meta.updatedAt` rather than a top-level column.
  val nestedTemporalSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField(
          "meta",
          StructType(Seq(StructField("updatedAt", TimestampType, nullable = true))),
          nullable = true)))

  /**
   * Writes a parquet file whose timestamp is nested under a `meta` struct. Id=1 appears twice so deduplication has
   * something to resolve: the 2024-06 row must win over the 2024-01 row.
   */
  private def writeNestedTemporalFile(name: String): String = {
    val rows =
      Seq(
        Row(1, 10.0, Row(java.sql.Timestamp.valueOf("2024-01-15 00:00:00"))),
        Row(2, 20.0, Row(java.sql.Timestamp.valueOf("2024-01-15 00:00:00"))),
        Row(1, 99.0, Row(java.sql.Timestamp.valueOf("2024-06-01 00:00:00"))))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), nestedTemporalSchema)
    val path = s"${System.getProperty("java.io.tmpdir")}/ariadne-nested-temporal-$name"
    df.write.mode("overwrite").parquet(path)
    path
  }

  test("temporal index supports a nested timestamp column") {
    val path = writeNestedTemporalFile("build")
    val index = Index("temporal_nested_timestamp", nestedTemporalSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addTemporalIndex("Id", "meta.updatedAt")

    // Regression: selecting a nested path flattens it to its leaf name, so aggregating by the
    // original dotted path used to fail here with UNRESOLVED_COLUMN.
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    val result = index.join(queryData, Seq("Id"), "inner")

    // Id=1 must resolve to the later 2024-06 row, not the 2024-01 one.
    val values = result.select("Id", "Value").collect().toSeq.map(r => (r.getInt(0), r.getDouble(1)))
    values should contain theSameElementsAs Seq((1, 99.0), (2, 20.0))
  }

  test("temporal deduplication works when a nested timestamp column is not selected") {
    val path = writeNestedTemporalFile("select")
    val index = Index("temporal_nested_timestamp_select", nestedTemporalSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addTemporalIndex("Id", "meta.updatedAt")
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("Id", IntegerType, nullable = false))))

    // The `meta` struct is dropped by the projection, so deduplication must still read it
    // internally and then remove it from the result.
    val result = index.select("Id", "Value").join(queryData, Seq("Id"), "inner")

    result.columns should contain theSameElementsAs Seq("Id", "Value")
    result.count() should be(1)
    result.select("Value").collect().head.getDouble(0) should be(99.0)
  }

  test("temporal index supports a value column named like an internal working column") {
    // Regression: the build aliased the timestamp to the fixed literal `_ariadne_ts`. A value
    // column with that exact name collided with the alias, making the aggregation ambiguous.
    val collidingSchema =
      StructType(
        Seq(
          StructField("_ariadne_ts", IntegerType, nullable = false),
          StructField("Value", DoubleType, nullable = false),
          StructField("updated_at", TimestampType, nullable = true)))

    val rows =
      Seq(
        Row(1, 10.0, java.sql.Timestamp.valueOf("2024-01-15 00:00:00")),
        Row(2, 20.0, java.sql.Timestamp.valueOf("2024-01-15 00:00:00")),
        Row(1, 99.0, java.sql.Timestamp.valueOf("2024-06-01 00:00:00")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), collidingSchema)
    val path = s"${System.getProperty("java.io.tmpdir")}/ariadne-temporal-alias-collision"
    df.write.mode("overwrite").parquet(path)

    val index = Index("temporal_alias_collision", collidingSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addTemporalIndex("_ariadne_ts", "updated_at")
    index.update

    val queryData =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("_ariadne_ts", IntegerType, nullable = false))))

    val result = index.join(queryData, Seq("_ariadne_ts"), "inner")

    val values = result.select("_ariadne_ts", "Value").collect().toSeq.map(r => (r.getInt(0), r.getDouble(1)))
    values should contain theSameElementsAs Seq((1, 99.0), (2, 20.0))
  }

  test("addTemporalIndex rejects a nested value column") {
    val index = Index("temporal_nested_value_rejected", nestedTemporalSchema, "parquet")

    // The value column is persisted under its own name, so a dotted path could never be read back.
    // It must be rejected up front rather than failing later during update.
    val error =
      the[IllegalArgumentException] thrownBy {
        index.addTemporalIndex("meta.updatedAt", "meta.updatedAt")
      }

    error.getMessage should include("Nested column 'meta.updatedAt'")
    error.getMessage should include("temporal index")
    index.indexes should not contain "meta.updatedAt"
  }

  test("index types that persist a value column reject nested columns") {
    val index = Index("nested_value_rejected_all_types", nestedTemporalSchema, "parquet")

    a[IllegalArgumentException] should be thrownBy index.addIndex("meta.updatedAt")
    a[IllegalArgumentException] should be thrownBy index.addBloomIndex("meta.updatedAt")
    a[IllegalArgumentException] should be thrownBy index.addRangeIndex("meta.updatedAt")
    a[IllegalArgumentException] should be thrownBy index.addComputedIndex("meta.derived", "Id + 1")

    index.indexes shouldBe empty
  }

  test("temporal dedup does not promote a stale row when pruning drops the newest file") {
    // A second indexed column narrows the file set by intersection, which can exclude the file
    // holding an entity's current version. Ranking only the rows that survived would then crown a
    // superseded row as "latest" and emit it as current.
    val mixedSchema =
      StructType(
        Seq(
          StructField("Id", IntegerType, nullable = false),
          StructField("Category", StringType, nullable = false),
          StructField("Value", DoubleType, nullable = false),
          StructField("UpdatedAt", TimestampType, nullable = true)))

    // stale.parquet holds Id=1's old version (Category A) and Id=4's current version (Category A).
    // current.parquet holds Id=1's current version, which moved to Category B.
    val stale =
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row(1, "A", 100.0, java.sql.Timestamp.valueOf("2024-01-15 10:00:00")),
            Row(4, "A", 400.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")))),
        mixedSchema)
    val current =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "B", 150.0, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")))),
        mixedSchema)

    val stalePath = s"${System.getProperty("java.io.tmpdir")}/temporal_stale_${System.currentTimeMillis()}"
    val currentPath = s"${System.getProperty("java.io.tmpdir")}/temporal_current_${System.currentTimeMillis()}"
    stale.coalesce(1).write.mode("overwrite").parquet(stalePath)
    current.coalesce(1).write.mode("overwrite").parquet(currentPath)

    try {
      val index = Index("temporal_stale_promotion", mixedSchema, "parquet")
      index.addFile(stalePath, currentPath)
      index.addTemporalIndex("Id", "UpdatedAt")
      index.addIndex("Category")
      index.update

      // Category=A prunes to stale.parquet alone, even though Id=1's current version lives elsewhere.
      val queryData =
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(1, "A"), Row(4, "A"))),
          StructType(Seq(StructField("Id", IntegerType, nullable = false), StructField("Category", StringType, false))))

      val result = index.join(queryData, Seq("Id", "Category"), "inner")
      val rows = result.select("Id", "Value").collect().toSeq.map(r => (r.getInt(0), r.getDouble(1)))

      // Id=1's current version is Category B, so it must not match a Category A query at all.
      rows should contain theSameElementsAs Seq((4, 400.0))
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(stalePath), true)
      fs.delete(new org.apache.hadoop.fs.Path(currentPath), true)
    }
  }
}
