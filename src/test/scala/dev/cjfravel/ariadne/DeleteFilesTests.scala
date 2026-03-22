package dev.cjfravel.ariadne

import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.Path

class DeleteFilesTests extends SparkTests with Matchers {

  val table1Schema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("should delete a single file from index") {
    val csvOptions = Map("header" -> "true")
    val index = Index("delete_single", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    // Both files should be locatable
    val beforeFiles = index.locateFiles(Map("Id" -> Array(1)))
    beforeFiles should contain(path0)

    index.deleteFiles(path0)

    // After delete, only path1 should remain
    val afterFiles = index.locateFiles(Map("Id" -> Array(1)))
    afterFiles should not contain path0
    afterFiles should contain(path1)
  }

  test("should delete multiple files from index") {
    val csvOptions = Map("header" -> "true")
    val index = Index("delete_multiple", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    index.deleteFiles(path0, path1)

    val afterFiles = index.locateFiles(Map("Id" -> Array(1)))
    afterFiles shouldBe empty
  }

  test("should clean up large index tables on delete") {
    val largeSchema = StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Category", StringType, nullable = false),
        StructField("Value", DoubleType, nullable = false)
      )
    )

    // Create data with enough distinct values to trigger large index (limit is 500,000)
    val largeData = (1 to 600000).map { i =>
      Row(i, s"cat_$i", i.toDouble)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(largeData),
      largeSchema
    )

    val tempPath = s"${System.getProperty("java.io.tmpdir")}/delete_large_test_${System.currentTimeMillis()}"
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(tempPath)

    try {
      val fileName = "file://" + java.nio.file.Files
        .walk(java.nio.file.Paths.get(tempPath))
        .filter(java.nio.file.Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".csv"))
        .findFirst()
        .get()
        .toString

      val index = Index("delete_large", largeSchema, "csv", Map("header" -> "true"))
      index.addFile(fileName)
      index.addIndex("Id")
      index.addIndex("Category")
      index.update

      // Verify large index exists before delete
      val largeIndexesPath = new Path(index.storagePath, "large_indexes")
      index.exists(largeIndexesPath) shouldBe true

      index.deleteFiles(fileName)

      // After delete, locateFiles should return nothing
      val afterFiles = index.locateFiles(Map("Id" -> Array(1)))
      afterFiles shouldBe empty

      // File should no longer be tracked
      index.hasFile(fileName) shouldBe false
    } finally {
      deleteRecursive(tempPath)
    }
  }

  test("should be a no-op for non-existent files") {
    val csvOptions = Map("header" -> "true")
    val index = Index("delete_noop", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    index.addFile(path0)
    index.addIndex("Id")
    index.update

    // Deleting a file that doesn't exist should not throw
    noException should be thrownBy {
      index.deleteFiles("file:///nonexistent/path.csv")
    }

    // Original file should still be intact
    val files = index.locateFiles(Map("Id" -> Array(1)))
    files should contain(path0)
  }

  test("should remove file from file list on delete") {
    val csvOptions = Map("header" -> "true")
    val index = Index("delete_filelist", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    index.hasFile(path0) shouldBe true
    index.deleteFiles(path0)
    index.hasFile(path0) shouldBe false
    index.hasFile(path1) shouldBe true
  }

  test("should handle delete on empty index") {
    val csvOptions = Map("header" -> "true")
    val index = Index("delete_empty", table1Schema, "csv", csvOptions)

    noException should be thrownBy {
      index.deleteFiles("file:///some/file.csv")
    }
  }

  private def deleteRecursive(path: String): Unit = {
    val dir = java.nio.file.Paths.get(path)
    if (java.nio.file.Files.exists(dir)) {
      java.nio.file.Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(java.nio.file.Files.delete)
    }
  }
}
