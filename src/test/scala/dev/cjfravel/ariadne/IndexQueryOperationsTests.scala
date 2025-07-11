package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class IndexQueryOperationsTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val arraySchema = StructType(Seq(
    StructField("event_id", StringType, nullable = true),
    StructField("users", ArrayType(StructType(Seq(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))), nullable = true),
    StructField("tags", ArrayType(StringType), nullable = true)
  ))

  test("should locate files by single index value") {
    val index = Index("query_single_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    val files = index.locateFiles(Map("Id" -> Array(1, 2)))
    files should not be empty
    files should contain(csvPath)
  }

  test("should locate files by multiple index values") {
    val index = Index("query_multiple_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath1 = resourcePath("/data/table1_part0.csv")
    val csvPath2 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath1, csvPath2)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update
    
    val files = index.locateFiles(Map("Id" -> Array(1), "Version" -> Array(1)))
    files should not be empty
  }

  test("should return empty set for non-existent values") {
    val index = Index("query_empty_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    val files = index.locateFiles(Map("Id" -> Array(999)))
    files should be(empty)
  }

  test("should locate files using computed indexes") {
    val index = Index("query_computed_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addComputedIndex("id_doubled", "Id * 2")
    index.update
    
    val files = index.locateFiles(Map("id_doubled" -> Array(2, 4))) // 2*1, 2*2
    files should not be empty
    files should contain(csvPath)
  }

  test("should locate files using exploded field indexes") {
    // Create test data with arrays
    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("evt1", Array(Row(100L, "Alice"), Row(101L, "Bob")), Array("tag1", "tag2")),
        Row("evt2", Array(Row(102L, "Charlie")), Array("tag3"))
      )),
      arraySchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/query_exploded_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("query_exploded_test", arraySchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addExplodedFieldIndex("users", "id", "user_id")
      index.update
      
      // Query using the storage column name (not the as_column name)
      val files = index.locateFiles(Map("users" -> Array(100L, 101L)))
      files should not be empty
      files.exists(_.contains("query_exploded_test")) should be(true)
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should generate statistics for indexed columns") {
    val index = Index("query_stats_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath1 = resourcePath("/data/table1_part0.csv")
    val csvPath2 = resourcePath("/data/table1_part1.csv")
    index.addFile(csvPath1, csvPath2)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update
    
    val stats = index.stats()
    stats.count() should be(1) // Should return one row of statistics
    
    val columns = stats.columns
    columns should contain("FileCount")
    columns should contain("Id")
    columns should contain("Version")
    
    // Verify FileCount is correct
    val fileCount = stats.select("FileCount").collect()(0).getAs[Long]("FileCount")
    fileCount should be(2) // Two files added
  }

  test("should handle statistics for empty index") {
    val index = Index("query_empty_stats_test", testSchema, "csv", Map("header" -> "true"))
    // Don't add any files or update
    
    val stats = index.stats()
    stats.count() should be(0) // Empty DataFrame for empty index
  }

  test("should provide statistics for computed indexes") {
    val index = Index("query_computed_stats_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addComputedIndex("id_doubled", "Id * 2")
    index.update
    
    val stats = index.stats()
    val columns = stats.columns
    columns should contain("Id")
    columns should contain("id_doubled")
  }

  test("should handle printIndex without errors") {
    val index = Index("query_print_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    // These should not throw exceptions
    index.printIndex(truncate = false)
    index.printIndex(truncate = true)
  }

  test("should handle printMetadata without errors") {
    val index = Index("query_metadata_print_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addComputedIndex("test_computed", "Id * 2")
    index.update
    
    // This should not throw an exception
    index.printMetadata
  }

  test("should handle large index merging") {
    // This test verifies that the large index merging logic works correctly
    // We create data that should trigger the large index path
    
    val largeData = (1 to 1000).map { i =>
      Row(i % 10, i % 5, i.toDouble) // Creates repeated values
    }
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(largeData),
      testSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/query_large_test_${System.currentTimeMillis()}"
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempPath)
    
    try {
      val fileName = java.nio.file.Files
        .walk(java.nio.file.Paths.get(tempPath))
        .filter(java.nio.file.Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".csv"))
        .findFirst()
        .get()
        .toString
      
      val index = Index("query_large_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile("file://" + fileName)
      index.addIndex("Id")
      index.update
      
      // Test that querying works even with large indexes
      val files = index.locateFiles(Map("Id" -> Array(1, 2)))
      files should not be empty
      
      // Test statistics work
      val stats = index.stats()
      stats.count() should be(1)
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle mixed queries across different index types") {
    val mixedSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("priority", IntegerType, nullable = true),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("role", StringType, nullable = true)
      ))), nullable = true)
    ))
    
    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("evt1", 1, Array(Row(100L, "admin"))),
        Row("evt2", 2, Array(Row(101L, "user"))),
        Row("evt3", 3, Array(Row(100L, "admin")))
      )),
      mixedSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/query_mixed_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("query_mixed_test", mixedSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addIndex("event_id") // Regular index
      index.addComputedIndex("priority_level", "case when priority > 2 then 'high' else 'low' end")
      index.addExplodedFieldIndex("users", "id", "user_id")
      index.update
      
      // Test regular index query
      val eventFiles = index.locateFiles(Map("event_id" -> Array("evt1", "evt2")))
      eventFiles should not be empty
      
      // Test computed index query
      val priorityFiles = index.locateFiles(Map("priority_level" -> Array("high")))
      priorityFiles should not be empty
      
      // Test exploded field query
      val userFiles = index.locateFiles(Map("users" -> Array(100L)))
      userFiles should not be empty
      
      // Test combined query
      val combinedFiles = index.locateFiles(Map(
        "event_id" -> Array("evt1"),
        "users" -> Array(100L)
      ))
      combinedFiles should not be empty
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }
}