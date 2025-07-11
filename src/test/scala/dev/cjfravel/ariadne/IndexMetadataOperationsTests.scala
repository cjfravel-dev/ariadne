package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import dev.cjfravel.ariadne.exceptions._
import java.util
import java.util.Collections

class IndexMetadataOperationsTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("format property should work correctly") {
    val testIndex = Index("format_test", testSchema, "csv")
    testIndex.format should be("csv")
  }

  test("stored schema should match original schema") {
    val testIndex = Index("schema_test", testSchema, "csv")
    testIndex.storedSchema should be(testSchema)
  }

  test("metadata should handle regular indexes") {
    val testIndex = Index("regular_index_test", testSchema, "csv")
    testIndex.addIndex("Id")
    testIndex.addIndex("Name")
    
    testIndex.indexes should contain("Id")
    testIndex.indexes should contain("Name")
    testIndex.indexes should not contain("Value")
  }

  test("metadata should handle exploded field indexes") {
    val arraySchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))), nullable = false)
    ))
    
    val testIndex = Index("exploded_test", arraySchema, "csv")
    testIndex.addIndex("Id")
    testIndex.addExplodedFieldIndex("users", "id", "user_id")
    
    testIndex.indexes should contain("Id")
    testIndex.indexes should contain("user_id")
  }

  test("metadata should handle computed indexes") {
    val testIndex = Index("computed_test", testSchema, "csv")
    testIndex.addComputedIndex("name_upper", "upper(Name)")
    testIndex.addComputedIndex("id_times_two", "Id * 2")
    
    testIndex.indexes should contain("name_upper")
    testIndex.indexes should contain("id_times_two")
  }

  test("metadata should handle read options") {
    val readOptions = Map("header" -> "true", "delimiter" -> ",")
    val testIndex = Index("read_options_test", testSchema, "csv", readOptions)
    
    // Verify the index was created successfully with read options
    testIndex.format should be("csv")
    testIndex.storedSchema should be(testSchema)
  }

  test("metadata caching should work with refreshMetadata") {
    val testIndex = Index("cache_test", testSchema, "csv")
    testIndex.addIndex("Id")
    
    // Verify metadata is accessible
    testIndex.format should be("csv")
    testIndex.storedSchema should be(testSchema)
    
    // Test refreshMetadata doesn't throw errors
    testIndex.refreshMetadata()
    testIndex.format should be("csv")
  }

  test("metadata should persist across index instances") {
    val indexName = "persistence_test"
    
    // Create first instance and add some indexes
    val index1 = Index(indexName, testSchema, "csv")
    index1.addIndex("Id")
    index1.addComputedIndex("test_computed", "upper(Name)")
    
    // Create second instance of same index
    val index2 = Index(indexName)
    
    // Should have same format and schema
    index2.format should be("csv")
    index2.storedSchema should be(testSchema)
    index2.indexes should contain("Id")
    index2.indexes should contain("test_computed")
  }

  test("metadata format validation should work") {
    val indexName = "format_validation_test"
    val index1 = Index(indexName, testSchema, "csv")
    
    // Should throw exception when trying to use different format
    assertThrows[FormatMismatchException] {
      val index2 = Index(indexName, testSchema, "parquet")
    }
  }

  test("metadata schema validation should work") {
    val indexName = "schema_validation_test"
    val index1 = Index(indexName, testSchema, "csv")
    
    val differentSchema = StructType(Seq(
      StructField("DifferentField", StringType, nullable = false)
    ))
    
    // Should throw exception when trying to use different schema
    assertThrows[SchemaMismatchException] {
      val index2 = Index(indexName, differentSchema, "csv")
    }
  }

  test("metadata should require schema for new index") {
    assertThrows[SchemaNotProvidedException] {
      val index = Index("no_schema_test")
    }
  }

  test("metadata should allow schema changes with allowSchemaMismatch flag") {
    val indexName = "schema_change_test"
    val index1 = Index(indexName, testSchema, "csv")
    index1.addIndex("Id")
    
    val newSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("NewField", StringType, nullable = false)
    ))
    
    // Should work with allowSchemaMismatch = true
    val index2 = Index(indexName, newSchema, "csv", true)
    index2.storedSchema should be(newSchema)
  }
}