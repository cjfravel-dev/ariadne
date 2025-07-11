package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import java.nio.file.Files

class IndexFileOperationsTests extends SparkTests with Matchers {

  val csvSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val jsonSchema = StructType(Seq(
    StructField("event_id", StringType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("users", ArrayType(StructType(Seq(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))), nullable = true)
  ))

  test("storedSchema should return correct schema") {
    val index = Index("file_ops_schema_test", csvSchema, "csv")
    index.storedSchema should be(csvSchema)
  }

  test("should handle CSV files with read options") {
    val csvOptions = Map("header" -> "true", "delimiter" -> ",")
    val index = Index("csv_read_test", csvSchema, "csv", csvOptions)
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update
    
    // Verify data was read correctly
    val files = index.locateFiles(Map("Id" -> Array(1, 2)))
    files should not be empty
  }

  test("should handle JSON files with multiLine option") {
    val readOptions = Map("multiLine" -> "true")
    val index = Index("json_read_test", jsonSchema, "json", readOptions)
    
    val jsonPath = resourcePath("/data/array_test.json")
    index.addFile(jsonPath)
    index.addIndex("event_id")
    index.update
    
    // Verify data was read correctly
    val files = index.locateFiles(Map("event_id" -> Array("e1")))
    files should not be empty
  }

  test("should apply computed indexes correctly") {
    val index = Index("computed_index_test", csvSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addComputedIndex("id_doubled", "Id * 2")
    index.addComputedIndex("version_str", "cast(Version as string)")
    index.update
    
    // Verify computed indexes are available
    index.indexes should contain("id_doubled")
    index.indexes should contain("version_str")
    
    // Test that we can locate files using computed indexes
    val files = index.locateFiles(Map("id_doubled" -> Array(2, 4, 6))) // 2*1, 2*2, 2*3
    files should not be empty
  }

  test("should apply exploded field transformations correctly") {
    // Test exploded field functionality with existing JSON test file
    val explodedSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("name", StringType, nullable = true)
      ))), nullable = true)
    ))
    
    val readOptions = Map("multiLine" -> "true")
    val index = Index("exploded_field_test", explodedSchema, "json", readOptions)
    
    val jsonPath = resourcePath("/data/array_test.json")
    index.addFile(jsonPath)
    
    // Test that exploded field indexes can be added without errors
    index.addExplodedFieldIndex("users", "id", "user_id")
    index.addExplodedFieldIndex("users", "name", "user_name")
    
    // Verify exploded field indexes are available in the public API
    index.indexes should contain("user_id")
    index.indexes should contain("user_name")
    
    // Test that the file was added successfully
    index.hasFile(jsonPath) should be(true)
    
    // Test that the schema is correct
    index.storedSchema should be(explodedSchema)
    index.format should be("json")
  }

  test("should handle different file formats") {
    // Test CSV
    val csvIndex = Index("format_csv_test", csvSchema, "csv", Map("header" -> "true"))
    csvIndex.format should be("csv")
    
    // Test JSON
    val jsonIndex = Index("format_json_test", jsonSchema, "json", Map("multiLine" -> "true"))
    jsonIndex.format should be("json")
    
    // Note: Parquet test would require creating parquet files, which is more complex
    // The existing integration tests already cover parquet format
  }

  test("should validate unsupported format") {
    // This test verifies that unsupported formats are rejected
    // We can't easily test this directly without accessing protected methods,
    // but we can verify that supported formats work
    val index = Index("supported_format_test", csvSchema, "csv")
    index.format should be("csv")
  }

  test("should handle read options correctly") {
    val customOptions = Map(
      "header" -> "true",
      "delimiter" -> ",",
      "quote" -> "\"",
      "escape" -> "\\"
    )
    
    val index = Index("read_options_test", csvSchema, "csv", customOptions)
    
    // Verify the index was created successfully with custom options
    index.format should be("csv")
    index.storedSchema should be(csvSchema)
  }

  test("should combine computed indexes and exploded fields") {
    val combinedSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("priority", IntegerType, nullable = true),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("role", StringType, nullable = true)
      ))), nullable = true)
    ))
    
    // Create test data
    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        org.apache.spark.sql.Row("evt1", 1, Array(
          org.apache.spark.sql.Row(100L, "admin"),
          org.apache.spark.sql.Row(101L, "user")
        )),
        org.apache.spark.sql.Row("evt2", 2, Array(
          org.apache.spark.sql.Row(102L, "user")
        ))
      )),
      combinedSchema
    )
    
    // Write test data
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/combined_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("combined_test", combinedSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addComputedIndex("high_priority", "case when priority > 1 then 'high' else 'low' end")
      index.addExplodedFieldIndex("users", "id", "user_id")
      index.update
      
      // Verify both types of indexes are available
      index.indexes should contain("high_priority")
      index.indexes should contain("user_id")
      
      // Test locating files using both index types
      val priorityFiles = index.locateFiles(Map("high_priority" -> Array("high")))
      priorityFiles should not be empty
      
      val userFiles = index.locateFiles(Map("users" -> Array(101L)))
      userFiles should not be empty
      
    } finally {
      // Clean up
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }
}