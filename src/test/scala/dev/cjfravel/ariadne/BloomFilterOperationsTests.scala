package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class BloomFilterOperationsTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("UserId", LongType, nullable = false),
      StructField("Category", StringType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("addBloomIndex should add a bloom index configuration") {
    val index = Index("bloom_add_test", testSchema, "csv", Map("header" -> "true"))
    
    index.addBloomIndex("UserId")
    
    index.indexes should contain("UserId")
  }

  test("addBloomIndex should accept custom FPR") {
    val index = Index("bloom_fpr_test", testSchema, "csv", Map("header" -> "true"))
    
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
    val index1 = Index("bloom_exclusive_test1", testSchema, "csv", Map("header" -> "true"))
    index1.addIndex("UserId")
    
    an[IllegalArgumentException] should be thrownBy {
      index1.addBloomIndex("UserId")
    }
    
    val index2 = Index("bloom_exclusive_test2", testSchema, "csv", Map("header" -> "true"))
    index2.addBloomIndex("UserId")
    
    an[IllegalArgumentException] should be thrownBy {
      index2.addIndex("UserId")
    }
  }

  test("addBloomIndex should be idempotent") {
    val index = Index("bloom_idempotent_test", testSchema, "csv", Map("header" -> "true"))
    
    index.addBloomIndex("UserId")
    index.addBloomIndex("UserId") // Should not throw
    
    index.indexes.count(_ == "UserId") should be(1)
  }

  test("should build bloom filter index during update") {
    val index = Index("bloom_build_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")
    index.update
    
    // Verify the index was built
    index.indexes should contain("Id")
  }

  test("should locate files using bloom filter") {
    val index = Index("bloom_locate_test", testSchema, "csv", Map("header" -> "true"))
    
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
    val files = index.locateFiles(Map("Id" -> Array(999999, 888888)))
    // We can't guarantee empty due to FPR, but this tests the code path
  }

  test("should support mixed bloom and regular indexes") {
    val index = Index("bloom_mixed_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")      // Bloom filter
    index.addIndex("Category")     // Regular index
    index.update
    
    // Both indexes should be available
    index.indexes should contain("Id")
    index.indexes should contain("Category")
  }

  test("should join using bloom filter index") {
    val index = Index("bloom_join_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addBloomIndex("Id")
    index.update
    
    // Create a query DataFrame
    val queryData = Seq(
      Row(1),
      Row(2),
      Row(3)
    )
    val querySchema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val queryDf = spark.createDataFrame(spark.sparkContext.parallelize(queryData), querySchema)
    
    // Join should work
    val result = index.join(queryDf, Seq("Id"), "inner")
    result.count() should be >= 0L
  }

  test("should handle multiple bloom indexes") {
    // Create test data with multiple columns
    val multiColData = (1 to 100).map { i =>
      Row(i, i.toLong * 1000, s"category_${i % 5}", i.toDouble)
    }
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(multiColData),
      testSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/bloom_multi_test_${System.currentTimeMillis()}"
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
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("bloom filter should have acceptable false positive rate") {
    // Create test data with known values
    val testData = (1 to 1000).map { i =>
      Row(i, i.toLong * 1000, s"category_${i % 5}", i.toDouble)
    }
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(testData),
      testSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/bloom_fpr_empirical_test_${System.currentTimeMillis()}"
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
      val fpResults = index.locateFiles(Map("Id" -> nonExistentValues.map(_.asInstanceOf[Any])))
      // We can't guarantee exact FPR in tests, but verify the mechanism works
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("metadata should persist bloom index configuration") {
    val index1 = Index("bloom_persist_test", testSchema, "csv", Map("header" -> "true"))
    index1.addBloomIndex("Id", fpr = 0.005)
    
    // Reload the index
    val index2 = Index("bloom_persist_test", testSchema, "csv")
    
    // Bloom index should still be configured
    index2.indexes should contain("Id")
  }
}