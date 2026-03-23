package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/** Tests for column backfill functionality, verifying that newly added regular
  * and computed index columns are correctly populated for previously indexed files.
  */
class ColumnBackfillTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("should backfill new regular index column on existing files") {
    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_regular", testSchema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.update

    // Verify initial state: Id is indexed, Version is not
    index.unindexedFiles should be(empty)
    val idFiles = index.locateFiles(Map("Id" -> Array(1)))
    idFiles should not be empty

    // Now add a new index column and update
    index.addIndex("Version")
    index.update

    // Verify Version is now queryable
    val versionFiles = index.locateFiles(Map("Version" -> Array(1)))
    versionFiles should not be empty
    versionFiles.size should be(2) // Both files contain Version=1

    // Verify Id still works
    val idFilesAfter = index.locateFiles(Map("Id" -> Array(1)))
    idFilesAfter should not be empty
    idFilesAfter should be(idFiles)
  }

  test("should backfill new computed index column on existing files") {
    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_computed", testSchema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.update

    // Add computed index after initial build
    index.addComputedIndex("id_doubled", "Id * 2")
    index.update

    // Verify computed index is populated
    val doubledFiles = index.locateFiles(Map("id_doubled" -> Array(2, 4)))
    doubledFiles should not be empty

    // Verify original index still works
    val idFiles = index.locateFiles(Map("Id" -> Array(1)))
    idFiles should not be empty
  }

  test("should backfill new bloom index column on existing files") {
    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_bloom", testSchema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.update

    // Add bloom index after initial build
    index.addBloomIndex("Version", fpr = 0.01)
    index.update

    // Verify bloom index is populated and queryable
    val bloomFiles = index.locateFiles(Map("Version" -> Array(1)))
    bloomFiles should not be empty

    // Verify original index still works
    val idFiles = index.locateFiles(Map("Id" -> Array(1)))
    idFiles should not be empty
  }

  test("should backfill new temporal index column on existing files") {
    val temporalSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false),
      StructField("UpdatedAt", TimestampType, nullable = true)
    ))

    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_temporal", temporalSchema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/temporal_part0.csv"),
      resourcePath("/data/temporal_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Value")
    index.update

    // Add temporal index after initial build
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // Verify temporal index is populated
    val temporalFiles = index.locateFiles(Map("Id" -> Array(1)))
    temporalFiles should not be empty

    // Verify original index still works
    val valueFiles = index.locateFiles(Map("Value" -> Array(100.0)))
    valueFiles should not be empty
  }

  test("should handle multiple new columns added at once") {
    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_multiple", testSchema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.update

    // Add two new columns at once
    index.addIndex("Version")
    index.addComputedIndex("id_tripled", "Id * 3")
    index.update

    // Verify both new columns are populated
    val versionFiles = index.locateFiles(Map("Version" -> Array(1)))
    versionFiles should not be empty

    val tripledFiles = index.locateFiles(Map("id_tripled" -> Array(3, 6)))
    tripledFiles should not be empty

    // Verify original index still works
    val idFiles = index.locateFiles(Map("Id" -> Array(1)))
    idFiles should not be empty
  }

  test("should be idempotent - second update after backfill is a no-op") {
    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_idempotent", testSchema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.update

    index.addIndex("Version")
    index.update

    // Second update should be a no-op
    index.filesNeedingColumnUpdate should be(empty)
    index.unindexedFiles should be(empty)
    index.update // Should not throw
  }

  test("should handle backfill with new files added simultaneously") {
    val csvOptions = Map("header" -> "true")
    val index = Index("backfill_with_new_files", testSchema, "csv", csvOptions)

    // First: index one file with col1
    val path0 = resourcePath("/data/table1_part0.csv")
    index.addFile(path0)
    index.addIndex("Id")
    index.update

    // Add new column AND new file at the same time
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path1)
    index.addIndex("Version")
    index.update

    // Both files should have both columns indexed
    val idFiles = index.locateFiles(Map("Id" -> Array(1)))
    idFiles should not be empty

    val versionFiles = index.locateFiles(Map("Version" -> Array(1)))
    versionFiles.size should be(2) // Both files contain Version=1
  }
}
