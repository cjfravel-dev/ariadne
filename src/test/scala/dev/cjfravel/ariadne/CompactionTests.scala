package dev.cjfravel.ariadne

import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._

/** Tests for `compact` and `vacuum` operations on the main index and large index
  * Delta tables.
  */
class CompactionTests extends SparkTests with Matchers {

  val testSchema: StructType = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val csvOptions: Map[String, String] = Map("header" -> "true")

  test("should compact main index") {
    val index = Index("compact_main_test", testSchema, "csv", csvOptions)
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")

    index.addFile(csvPath0, csvPath1)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    val filesBefore = index.locateFiles(Map("Id" -> Array(1, 2)))
    filesBefore should not be empty

    // compact should not throw
    index.compact()

    // index should still work after compaction
    val filesAfter = index.locateFiles(Map("Id" -> Array(1, 2)))
    filesAfter shouldEqual filesBefore
  }

  test("should compact large index tables") {
    // Use a very small largeIndexLimit so the index is forced into large_indexes
    spark.conf.set("spark.ariadne.largeIndexLimit", "1")
    try {
      val index = Index("compact_large_test", testSchema, "csv", csvOptions)
      val csvPath0 = resourcePath("/data/table1_part0.csv")

      index.addFile(csvPath0)
      index.addIndex("Id")
      index.update

      val filesBefore = index.locateFiles(Map("Id" -> Array(1, 2)))
      filesBefore should not be empty

      // compact should handle large index tables without error
      index.compact()

      val filesAfter = index.locateFiles(Map("Id" -> Array(1, 2)))
      filesAfter shouldEqual filesBefore
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }
  }

  test("should handle compact on empty index") {
    val index = Index("compact_empty_test", testSchema, "csv", csvOptions)
    index.addIndex("Id")

    // compact before any data — should not throw
    noException should be thrownBy index.compact()
  }

  test("should auto-compact when threshold is reached") {
    // Set a very low threshold (1 log file triggers compaction)
    spark.conf.set("spark.ariadne.autoCompactThreshold", "1")
    try {
      val index = Index("compact_auto_test", testSchema, "csv", csvOptions)
      val csvPath0 = resourcePath("/data/table1_part0.csv")

      index.addFile(csvPath0)
      index.addIndex("Id")
      // update triggers maybeAutoCompact internally
      index.update

      // After update+auto-compact the index should still be queryable
      val files = index.locateFiles(Map("Id" -> Array(1, 2)))
      files should not be empty
    } finally {
      spark.conf.unset("spark.ariadne.autoCompactThreshold")
    }
  }

  test("should not auto-compact when threshold is not set") {
    // Ensure config is absent
    spark.conf.unset("spark.ariadne.autoCompactThreshold")

    val index = Index("compact_no_auto_test", testSchema, "csv", csvOptions)
    val csvPath0 = resourcePath("/data/table1_part0.csv")

    index.addFile(csvPath0)
    index.addIndex("Id")
    // update should complete without auto-compaction errors
    noException should be thrownBy index.update

    val files = index.locateFiles(Map("Id" -> Array(1, 2)))
    files should not be empty
  }

  test("should vacuum with retention") {
    val index = Index("compact_vacuum_test", testSchema, "csv", csvOptions)
    val csvPath0 = resourcePath("/data/table1_part0.csv")
    val csvPath1 = resourcePath("/data/table1_part1.csv")

    index.addFile(csvPath0, csvPath1)
    index.addIndex("Id")
    index.update

    val filesBefore = index.locateFiles(Map("Id" -> Array(1, 2)))
    filesBefore should not be empty

    // vacuum with default retention should not throw
    noException should be thrownBy index.vacuum()

    // vacuum with 0-hour retention should also work
    noException should be thrownBy index.vacuum(retentionHours = 0)

    // index should still work after vacuum
    val filesAfter = index.locateFiles(Map("Id" -> Array(1, 2)))
    filesAfter shouldEqual filesBefore
  }
}
