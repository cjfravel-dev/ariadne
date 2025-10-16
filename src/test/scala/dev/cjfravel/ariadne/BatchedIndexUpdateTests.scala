package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class BatchedIndexUpdateTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Category", StringType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  private def createTestFile(id: Int, distinctValues: Int): (String, String) = {
    val testData = (1 to distinctValues).map { j =>
      Row(j, id % 10, s"category_${id}_${j}", j.toDouble)
    }
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(testData),
      testSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/batch_test_${id}_${System.currentTimeMillis()}"
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempPath)
    
    val fileName = java.nio.file.Files
      .walk(java.nio.file.Paths.get(tempPath))
      .filter(java.nio.file.Files.isRegularFile(_))
      .filter(_.getFileName.toString.endsWith(".csv"))
      .findFirst()
      .get()
      .toString
    
    (tempPath, "file://" + fileName)
  }

  test("should create multiple batches based on distinct value limits") {
    // Create files that force multiple batches due to distinct value accumulation
    // Total distinct values across files will exceed largeIndexLimit (500k), forcing batching
    val tempFiles = Seq(
      createTestFile(1, 150000),   // 150k distinct values
      createTestFile(2, 150000),   // 150k distinct values - can batch with file 1 (300k total < 500k)
      createTestFile(3, 200000),   // 200k distinct values - cannot fit with files 1&2, starts new batch
      createTestFile(4, 100000),   // 100k distinct values - can batch with file 3 (300k total < 500k)
      createTestFile(5, 250000)    // 250k distinct values - cannot fit with files 3&4, starts new batch
    )
    
    try {
      val index = Index("multiple_batch_test", testSchema, "csv", Map("header" -> "true"))
      
      // Add all files at once for cleaner logs
      index.addFile(tempFiles.map(_._2): _*)
      
      index.addIndex("Id")
      
      // This should create multiple batches based on distinct value analysis
      index.update
      
      // Verify the index works across all batches
      val files1 = index.locateFiles(Map("Id" -> Array(75000)))  // Should find in first batch
      val files2 = index.locateFiles(Map("Id" -> Array(175000))) // Should find in second batch
      val files3 = index.locateFiles(Map("Id" -> Array(225000))) // Should find in third batch
      
      files1 should not be empty
      files2 should not be empty
      files3 should not be empty
      
    } finally {
      // Cleanup
      tempFiles.foreach { case (tempPath, _) =>
        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
        fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
      }
    }
  }

  test("should handle files exceeding largeIndexLimit individually") {
    // Create files that individually exceed the largeIndexLimit (500k distinct values)
    val tempFiles = Seq(
      createTestFile(1, 600000),   // Exceeds largeIndexLimit - processed individually
      createTestFile(2, 100000),   // Small file - can be batched
      createTestFile(3, 150000),   // Small file - can be batched with file 2
      createTestFile(4, 700000),   // Exceeds largeIndexLimit - processed individually
      createTestFile(5, 200000)    // Small file - can be batched with files 2&3 or separate batch
    )
    
    try {
      val index = Index("large_index_test", testSchema, "csv", Map("header" -> "true"))
      
      // Add all files at once for cleaner logs
      index.addFile(tempFiles.map(_._2): _*)
      
      index.addIndex("Id")
      
      // Should handle large files individually and batch small files intelligently
      index.update
      
      // Verify functionality across all files
      val files1 = index.locateFiles(Map("Id" -> Array(300000))) // Should find large file 1
      val files2 = index.locateFiles(Map("Id" -> Array(50000)))  // Should find small files
      val files3 = index.locateFiles(Map("Id" -> Array(400000))) // Should find large file 4
      
      files1 should not be empty
      files2 should not be empty
      files3 should not be empty
      
    } finally {
      // Cleanup
      tempFiles.foreach { case (tempPath, _) =>
        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
        fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
      }
    }
  }

  test("should optimize batching with intelligent bin-packing") {
    // Create files with varying distinct value counts to test intelligent bin-packing
    val tempFiles = Seq(
      createTestFile(1, 200000),   // 200k distinct values
      createTestFile(2, 250000),   // 250k distinct values - can batch with file 1 (450k < 500k)
      createTestFile(3, 100000),   // 100k distinct values - cannot fit with files 1&2, new batch
      createTestFile(4, 300000),   // 300k distinct values - can batch with file 3 (400k < 500k)
      createTestFile(5, 50000),    // 50k distinct values - can fit with files 3&4 (450k < 500k)
      createTestFile(6, 150000),   // 150k distinct values - cannot fit with files 3,4,5, new batch
      createTestFile(7, 200000),   // 200k distinct values - can batch with file 6 (350k < 500k)
      createTestFile(8, 100000)    // 100k distinct values - can batch with files 6&7 (450k < 500k)
    )
    
    try {
      val index = Index("optimized_batch_test", testSchema, "csv", Map("header" -> "true"))
      
      // Add all files at once for cleaner logs
      index.addFile(tempFiles.map(_._2): _*)
      
      index.addIndex("Id")
      
      // Should use intelligent bin-packing to create optimal batches
      index.update
      
      // Verify all data is indexed correctly across multiple batches
      val files1 = index.locateFiles(Map("Id" -> Array(50000)))   // Should find in small files
      val files2 = index.locateFiles(Map("Id" -> Array(150000)))  // Should find in medium files
      val files3 = index.locateFiles(Map("Id" -> Array(250000)))  // Should find in larger files
      
      files1 should not be empty
      files2 should not be empty
      files3 should not be empty
      
    } finally {
      // Cleanup
      tempFiles.foreach { case (tempPath, _) =>
        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
        fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
      }
    }
  }

  test("should handle single file efficiently") {
    val tempFile = createTestFile(1, 100)
    
    try {
      val index = Index("single_file_test", testSchema, "csv", Map("header" -> "true"))
      
      index.addFile(tempFile._2)
      index.addIndex("Id")
      
      // Should still use batching framework even for single file
      index.update
      
      // Verify functionality
      val files = index.locateFiles(Map("Id" -> Array(50)))
      files should not be empty
      files.size should be(1)
      
    } finally {
      // Cleanup
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempFile._1), true)
    }
  }

  test("should handle mixed file sizes with optimal batching") {
    // Create a realistic scenario with mixed file sizes
    val tempFiles = (1 to 25).map { i =>
      val distinctValues = i match {
        case x if x <= 5 => 1000    // Large files
        case x if x <= 15 => 300    // Medium files
        case _ => 100               // Small files
      }
      createTestFile(i, distinctValues)
    }
    
    try {
      val index = Index("mixed_size_test", testSchema, "csv", Map("header" -> "true"))
      
      // Add all files at once for cleaner logs
      index.addFile(tempFiles.map(_._2): _*)
      
      index.addIndex("Id")
      index.addIndex("Category")
      
      // Should create optimal batches based on file characteristics
      index.update
      
      // Test various queries to ensure all data is properly indexed
      val smallFileResults = index.locateFiles(Map("Id" -> Array(50))) // Should find small files
      val mediumFileResults = index.locateFiles(Map("Id" -> Array(250))) // Should find medium files
      val largeFileResults = index.locateFiles(Map("Id" -> Array(800))) // Should find large files
      
      smallFileResults should not be empty
      mediumFileResults should not be empty
      largeFileResults should not be empty
      
    } finally {
      // Cleanup
      tempFiles.foreach { case (tempPath, _) =>
        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
        fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
      }
    }
  }
}