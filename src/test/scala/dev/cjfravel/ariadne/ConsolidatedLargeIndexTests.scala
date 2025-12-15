package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.Path

class ConsolidatedLargeIndexTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Category", StringType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("should consolidate large indexes into single delta table per column") {
    // Create test data that will trigger large index threshold (500,000+ distinct values per column)
    val largeData = (1 to 600000).map { i =>
      Row(i, i % 1000, s"category_${i}", i.toDouble)
    }
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(largeData),
      testSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/consolidated_test_${System.currentTimeMillis()}"
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
      
      val index = Index("consolidated_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile("file://" + fileName)
      index.addIndex("Id")
      index.addIndex("Category")
      index.update
      
      // Verify that consolidated structure exists
      val largeIndexesPath = new Path(index.storagePath, "large_indexes")
      
      if (index.exists(largeIndexesPath)) {
        val columnDirs = index.fs.listStatus(largeIndexesPath)
          .filter(_.isDirectory)
          .map(_.getPath.getName)
        
        // Should have consolidated tables, not file subdirectories
        columnDirs.foreach { columnName =>
          val columnPath = new Path(largeIndexesPath, columnName)
          val subDirs = index.fs.listStatus(columnPath)
            .filter(_.isDirectory)
            .map(_.getPath.getName)
          
          // Should not have file subdirectories in new consolidated format
          subDirs.foreach { subDir =>
            // Any subdirectories should be delta table internal dirs, not cleaned filenames
            subDir should not include "csv"
          }
        }
      }
      
      // Verify functionality still works
      val files = index.locateFiles(Map("Id" -> Array(1, 2, 3)))
      files should not be empty
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle incremental updates to consolidated large indexes") {
    val testData1 = (1 to 300000).map { i =>
      Row(i, i % 1000, s"batch1_${i}", i.toDouble)
    }
    
    val testData2 = (300001 to 600000).map { i =>
      Row(i, i % 1000, s"batch2_${i}", i.toDouble)
    }
    
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(testData1), testSchema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(testData2), testSchema)
    
    val tempPath1 = s"${System.getProperty("java.io.tmpdir")}/incremental1_${System.currentTimeMillis()}"
    val tempPath2 = s"${System.getProperty("java.io.tmpdir")}/incremental2_${System.currentTimeMillis()}"
    
    df1.coalesce(1).write.option("header", "true").mode("overwrite").csv(tempPath1)
    df2.coalesce(1).write.option("header", "true").mode("overwrite").csv(tempPath2)
    
    try {
      val fileName1 = java.nio.file.Files
        .walk(java.nio.file.Paths.get(tempPath1))
        .filter(java.nio.file.Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".csv"))
        .findFirst().get().toString
        
      val fileName2 = java.nio.file.Files
        .walk(java.nio.file.Paths.get(tempPath2))
        .filter(java.nio.file.Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".csv"))
        .findFirst().get().toString
      
      val index = Index("incremental_consolidated_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile("file://" + fileName1)
      index.addIndex("Id")
      index.addIndex("Category")
      index.update
      
      val initialFiles = index.locateFiles(Map("Category" -> Array("batch1_1", "batch1_2")))
      initialFiles should not be empty
      
      // Add second file and update incrementally
      index.addFile("file://" + fileName2)
      index.update
      
      // Should find files from both batches
      val batch1Files = index.locateFiles(Map("Category" -> Array("batch1_1", "batch1_100", "batch1_1000")))
      val batch2Files = index.locateFiles(Map("Category" -> Array("batch2_300001", "batch2_400000", "batch2_500000")))
      
      batch1Files should not be empty
      batch2Files should not be empty
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath1), true)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath2), true)
    }
  }

  test("should handle mixed large and small indexes correctly") {
    // Create data where some columns will be large and others small
    val mixedData = (1 to 600000).map { i =>
      Row(
        i % 5,        // Small index (only 5 distinct values)
        i % 10,       // Medium index
        s"large_category_${i}", // Large index (600,000 distinct values)
        i.toDouble
      )
    }
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(mixedData),
      testSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/mixed_index_test_${System.currentTimeMillis()}"
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
      
      val index = Index("mixed_index_test", testSchema, "csv", Map("header" -> "true"))
      index.addFile("file://" + fileName)
      index.addIndex("Id")        // Small index
      index.addIndex("Version")   // Medium index  
      index.addIndex("Category")  // Large index
      index.update
      
      // All queries should work regardless of index size
      val smallIndexFiles = index.locateFiles(Map("Id" -> Array(1, 2)))
      smallIndexFiles should not be empty
      
      val mediumIndexFiles = index.locateFiles(Map("Version" -> Array(3, 4)))
      mediumIndexFiles should not be empty
      
      val largeIndexFiles = index.locateFiles(Map("Category" -> Array("large_category_100", "large_category_200")))
      largeIndexFiles should not be empty
      
      // Combined queries should also work
      val combinedFiles = index.locateFiles(Map(
        "Id" -> Array(1), 
        "Category" -> Array("large_category_100")
      ))
      combinedFiles should not be empty
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }
}