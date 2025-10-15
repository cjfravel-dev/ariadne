package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import dev.cjfravel.ariadne.exceptions.InvalidColumnSelectionException

class IndexSelectOperationsTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false),
      StructField("Category", StringType, nullable = false),
      StructField("Status", StringType, nullable = false)
    )
  )

  val querySchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false)
    )
  )

  test("should create SelectedIndex with valid columns") {
    val index = Index("select_basic_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update

    val selectedIndex = index.select("Id", "Version", "Value")
    selectedIndex.selectedColumns should contain allOf("Id", "Version", "Value")
    selectedIndex.name should be(index.name)
    selectedIndex.schema should be(index.schema)
  }

  test("should throw exception for invalid column selection") {
    val index = Index("select_invalid_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    assertThrows[InvalidColumnSelectionException] {
      index.select("Id", "NonExistentColumn")
    }
  }

  test("should allow further column restriction on SelectedIndex") {
    val index = Index("select_restrict_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update

    val selectedIndex = index.select("Id", "Version", "Value")
    val furtherRestricted = selectedIndex.select("Id", "Version")
    
    furtherRestricted.selectedColumns should contain allOf("Id", "Version")
    furtherRestricted.selectedColumns should not contain("Value")
  }

  test("should throw exception when restricting to columns not in current selection") {
    val index = Index("select_restrict_invalid_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    val selectedIndex = index.select("Id", "Version")
    
    assertThrows[InvalidColumnSelectionException] {
      selectedIndex.select("Id", "Value") // Value not in current selection
    }
  }

  test("should filter indexes based on selected columns") {
    val index = Index("select_filter_indexes_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update

    val selectedIndex = index.select("Id", "Version")
    
    // Only indexed columns that are also selected should be available
    selectedIndex.indexes should contain allOf("Id", "Version")
    selectedIndex.indexes should not contain("Value")
  }

  test("should perform joins with column filtering") {
    val index = Index("select_join_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update

    val selectedIndex = index.select("Id", "Version")

    // Create query DataFrame
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 1),
        Row(2, 2)
      )),
      querySchema
    )

    val result = selectedIndex.join(queryData, Seq("Id", "Version"), "inner")
    
    // Result should contain only selected columns plus join columns
    result.columns should contain allOf("Id", "Version")
    result.columns should not contain("Value")
    result.count() should be > 0L
  }

  test("should locate files using only selected indexed columns") {
    val index = Index("select_locate_files_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update

    val selectedIndex = index.select("Id", "Version")

    // Should only use Id and Version for file location, ignoring Value
    val files1 = selectedIndex.locateFiles(Map(
      "Id" -> Array(1, 2),
      "Version" -> Array(1, 2),
      "Value" -> Array(1.0, 2.0) // This should be ignored
    ))

    val files2 = selectedIndex.locateFiles(Map(
      "Id" -> Array(1, 2),
      "Version" -> Array(1, 2)
    ))

    files1 should equal(files2) // Results should be the same since Value is ignored
  }

  test("should prevent modification operations on SelectedIndex") {
    val index = Index("select_modification_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    val selectedIndex = index.select("Id", "Version")

    // All modification operations should throw exceptions
    assertThrows[UnsupportedOperationException] {
      selectedIndex.addFile("some_file.csv")
    }

    assertThrows[UnsupportedOperationException] {
      selectedIndex.addIndex("NewIndex")
    }

    assertThrows[UnsupportedOperationException] {
      selectedIndex.addComputedIndex("computed", "Id * 2")
    }

    assertThrows[UnsupportedOperationException] {
      selectedIndex.addExplodedFieldIndex("array_col", "field", "alias")
    }

    assertThrows[UnsupportedOperationException] {
      selectedIndex.update
    }
  }

  test("should allow read-only operations on SelectedIndex") {
    val index = Index("select_readonly_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    val selectedIndex = index.select("Id", "Version")

    // These operations should work without exceptions
    selectedIndex.hasFile(csvPath) should be(true)
    selectedIndex.format should be("csv")
    selectedIndex.stats() // Should not throw
    selectedIndex.refreshMetadata() // Should not throw
  }

  test("should work with computed indexes") {
    val index = Index("select_computed_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addComputedIndex("id_doubled", "Id * 2")
    index.update

    val selectedIndex = index.select("Id", "id_doubled")
    
    selectedIndex.indexes should contain allOf("Id", "id_doubled")

    // Create query data for computed index
    val computedQuerySchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("id_doubled", IntegerType, nullable = false)
    ))
    
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 2), // Id=1, id_doubled=2
        Row(2, 4)  // Id=2, id_doubled=4
      )),
      computedQuerySchema
    )

    val result = selectedIndex.join(queryData, Seq("Id", "id_doubled"), "inner")
    result.count() should be > 0L
  }

  test("should work with exploded field indexes") {
    val explodedSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("priority", IntegerType, nullable = true),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("name", StringType, nullable = true)
      ))), nullable = true)
    ))

    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("evt1", 1, Array(Row(100L, "Alice"))),
        Row("evt2", 2, Array(Row(101L, "Bob")))
      )),
      explodedSchema
    )

    val tempPath = s"${System.getProperty("java.io.tmpdir")}/select_exploded_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)

    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("select_exploded_test", explodedSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addIndex("event_id")
      index.addExplodedFieldIndex("users", "id", "user_id")
      index.update

      val selectedIndex = index.select("event_id", "user_id")
      
      selectedIndex.indexes should contain allOf("event_id", "user_id")

      // Create query data
      val userQuerySchema = StructType(Seq(
        StructField("event_id", StringType, nullable = false),
        StructField("user_id", LongType, nullable = false)
      ))
      
      val queryData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("evt1", 100L),
          Row("evt2", 101L)
        )),
        userQuerySchema
      )

      val result = selectedIndex.join(queryData, Seq("event_id", "user_id"), "inner")
      result.count() should be > 0L
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle empty column selection") {
    val index = Index("select_empty_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update

    // Empty selection should be allowed but not very useful
    val selectedIndex = index.select()
    selectedIndex.selectedColumns should be(empty)
    selectedIndex.indexes should be(empty)
  }

  test("should handle selection with no indexed columns") {
    val index = Index("select_no_indexes_test", testSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    // Don't add any indexes
    index.update

    val selectedIndex = index.select("Id", "Version")
    selectedIndex.selectedColumns should contain allOf("Id", "Version")
    selectedIndex.indexes should be(empty) // No indexes available
  }
}