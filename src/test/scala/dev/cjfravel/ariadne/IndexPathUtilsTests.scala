package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import dev.cjfravel.ariadne.exceptions.IndexNotFoundException

/** Tests for index path utility methods covering file list naming, special character
  * handling in file names, path existence checks, and directory removal.
  */
class IndexPathUtilsTests extends SparkTests with Matchers {

  val basicSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", StringType, nullable = false)
    )
  )

  test("fileListName should format correctly") {
    IndexPathUtils.fileListName("test_index") should be("[ariadne_index] test_index")
    IndexPathUtils.fileListName("my-index") should be("[ariadne_index] my-index")
    assertThrows[IllegalArgumentException] {
      IndexPathUtils.fileListName("")
    }
  }

  test("cleanFileName should handle special characters") {
    IndexPathUtils.cleanFileName("file.txt") should be("file_txt")
    IndexPathUtils.cleanFileName("file-name_123.csv") should be("file_name_123_csv")
    IndexPathUtils.cleanFileName("file@#$%^&*()name") should be("file_name")
    IndexPathUtils.cleanFileName("___file___") should be("file")
    IndexPathUtils.cleanFileName("file____name") should be("file_name")
  }

  test("cleanFileName should handle edge cases") {
    IndexPathUtils.cleanFileName("") should be("")
    IndexPathUtils.cleanFileName("___") should be("")
    IndexPathUtils.cleanFileName("123abc") should be("123abc")
    IndexPathUtils.cleanFileName("ABC") should be("ABC")
  }

  test("cleanFileName should preserve alphanumeric characters") {
    IndexPathUtils.cleanFileName("file123name") should be("file123name")
    IndexPathUtils.cleanFileName("File123Name") should be("File123Name")
    IndexPathUtils.cleanFileName("123") should be("123")
    IndexPathUtils.cleanFileName("abc") should be("abc")
  }

  test("storagePath should be correct") {
    val expectedPath = new Path(IndexPathUtils.storagePath.getParent, "indexes")
    IndexPathUtils.storagePath.toString should endWith("indexes")
  }

  test("exists should work for non-existent index") {
    IndexPathUtils.exists("nonexistent_index") should be(false)
  }

  test("remove should throw exception for non-existent index") {
    assertThrows[IndexNotFoundException] {
      IndexPathUtils.remove("nonexistent_index")
    }
  }

  test("exists and remove should work with actual index") {
    // Create a simple index for testing
    val testIndex = Index("path_utils_test", basicSchema, "csv")
    testIndex.addFile("dummy.csv")
    
    // Test exists
    IndexPathUtils.exists("path_utils_test") should be(true)
    
    // Test remove
    val removed = IndexPathUtils.remove("path_utils_test")
    removed should be(true)
    
    // Verify it no longer exists
    IndexPathUtils.exists("path_utils_test") should be(false)
  }

  test("validateIndexName rejects unsafe names") {
    val bad = Seq("../escape", "a/b", "a\\b", ".hidden", "..", "a\u0000b", "", " ")
    bad.foreach { name =>
      withClue(s"name='$name' should be rejected: ") {
        assertThrows[IllegalArgumentException] {
          IndexPathUtils.validateIndexName(name)
        }
      }
    }
    assertThrows[IllegalArgumentException] {
      IndexPathUtils.validateIndexName(null)
    }
  }

  test("validateIndexName accepts safe names") {
    Seq("orders", "my_index", "customer-data", "v2.final", "A1B2").foreach { name =>
      IndexPathUtils.validateIndexName(name)
    }
  }
}