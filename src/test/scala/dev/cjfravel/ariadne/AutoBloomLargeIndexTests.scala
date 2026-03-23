package dev.cjfravel.ariadne

import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._
import java.nio.charset.StandardCharsets

/** Tests for automatic bloom filter creation on large index columns, verifying
  * that auto-bloom filters are built during `update` and used to pre-filter large index queries.
  */
class AutoBloomLargeIndexTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  private def readMetadata(index: Index): IndexMetadata = {
    val metadataPath = new Path(index.storagePath, "metadata.json")
    val stream = index.open(metadataPath)
    try {
      val bytes = new Array[Byte](stream.available())
      stream.readFully(bytes)
      IndexMetadata(new String(bytes, StandardCharsets.UTF_8))
    } finally {
      stream.close()
    }
  }

  test("should automatically create bloom filter for large columns") {
    spark.conf.set("spark.ariadne.largeIndexLimit", "1")
    try {
      val index = Index("auto_bloom_create_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addIndex("Id")
      index.update

      // Verify auto_bloom column exists in the main index
      val indexDf = spark.read.format("delta").load(new Path(index.storagePath, "index").toString)
      indexDf.columns should contain("auto_bloom_Id")

      // Verify metadata tracks auto_bloom_indexes
      val meta = readMetadata(index)
      meta.auto_bloom_indexes.asScala should contain("Id")
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }
  }

  test("should use auto-bloom to filter large index queries") {
    spark.conf.set("spark.ariadne.largeIndexLimit", "1")
    try {
      val index = Index("auto_bloom_query_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addFile(resourcePath("/data/table1_part1.csv"))
      index.addIndex("Id")
      index.update

      // part0 has IDs {1, 2, 3}, part1 has IDs {1, 3, 4}

      // Query with a value that exists only in part0 (Id=2)
      val files = index.locateFiles(Map("Id" -> Array(2)))
      files should not be empty
      files.size shouldBe 1

      // Query with a value that exists in both parts (Id=3)
      val filesForId3 = index.locateFiles(Map("Id" -> Array(3)))
      filesForId3 should not be empty
      filesForId3.size shouldBe 2

      // Query with a value that exists only in part1 (Id=4)
      val filesForId4 = index.locateFiles(Map("Id" -> Array(4)))
      filesForId4 should not be empty
      filesForId4.size shouldBe 1
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }
  }

  test("should handle backward compatibility with old metadata") {
    // Load v7 metadata (no auto_bloom_indexes field)
    val stream = getClass.getResourceAsStream("/index_metadata/v7.json")
    require(stream != null, "Resource not found: /index_metadata/v7.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)

    // Migration should create empty auto_bloom_indexes
    metadata.auto_bloom_indexes should not be null
    metadata.auto_bloom_indexes.size() shouldBe 0
  }

  test("should not create auto-bloom for columns under limit") {
    // Use the default large limit so nothing triggers auto-bloom
    spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    val index = Index("auto_bloom_no_trigger_test", testSchema, "csv", Map("header" -> "true"))
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addIndex("Id")
    index.update

    // No columns should have auto-bloom since all are well under 500,000
    val meta = readMetadata(index)
    meta.auto_bloom_indexes.size() shouldBe 0

    // Index should still work normally
    val files = index.locateFiles(Map("Id" -> Array(1)))
    files should not be empty
  }

  test("should correctly identify files with auto-bloom filter via DataFrame join") {
    spark.conf.set("spark.ariadne.largeIndexLimit", "1")
    try {
      val index = Index("auto_bloom_join_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addFile(resourcePath("/data/table1_part1.csv"))
      index.addIndex("Id")
      index.update

      // Create a query DataFrame with specific Id values
      val _spark = spark
      import _spark.implicits._
      val queryDf = Seq(2).toDF("Id")

      // Join using the index
      val result = index.join(queryDf, Seq("Id"))
      result.count() should be > 0L

      // Verify correct data returned
      val ids = result.select("Id").collect().map(_.getInt(0)).toSet
      ids should contain(2)
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }
  }

  test("should work with multiple auto-bloom columns") {
    spark.conf.set("spark.ariadne.largeIndexLimit", "1")
    try {
      val index = Index("auto_bloom_multi_col_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/table1_part0.csv"))
      index.addFile(resourcePath("/data/table1_part1.csv"))
      index.addIndex("Id")
      index.addIndex("Version")
      index.update

      // Both columns should have auto-bloom
      val meta = readMetadata(index)
      meta.auto_bloom_indexes.asScala should contain("Id")
      meta.auto_bloom_indexes.asScala should contain("Version")

      // Verify index has both auto_bloom columns
      val indexDf = spark.read.format("delta").load(new Path(index.storagePath, "index").toString)
      indexDf.columns should contain("auto_bloom_Id")
      indexDf.columns should contain("auto_bloom_Version")

      // Multi-column query should work
      val files = index.locateFiles(Map("Id" -> Array(1), "Version" -> Array(1)))
      files should not be empty
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }
  }
}
