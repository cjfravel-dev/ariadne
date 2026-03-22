package dev.cjfravel.ariadne

import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class MultiColumnIntersectTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  test("should intersect files when querying multiple columns") {
    val index = Index("intersect_multi", testSchema, "csv", Map("header" -> "true"))

    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    // Id=4 is in part1 only. Version=1 is in both part0 and part1.
    // Intersection should yield only part1.
    val files = index.locateFiles(Map("Id" -> Array(4), "Version" -> Array(1)))
    files should have size 1
    files should contain(path1)
  }

  test("should return all files when single column matches all") {
    val index = Index("intersect_all", testSchema, "csv", Map("header" -> "true"))

    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    // Id=1 is in both files
    val files = index.locateFiles(Map("Id" -> Array(1)))
    files should have size 2
    files should contain allOf (path0, path1)
  }

  test("should return empty when intersection is empty") {
    val index = Index("intersect_empty", testSchema, "csv", Map("header" -> "true"))

    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    // Id=2 is in part0 only. Version=3 is in part1 only.
    // Intersection is empty.
    val files = index.locateFiles(Map("Id" -> Array(2), "Version" -> Array(3)))
    files shouldBe empty
  }

  test("should intersect in DataFrame-based join") {
    val index = Index("intersect_join", testSchema, "csv", Map("header" -> "true"))

    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    // Query for Id=4, Version=2 — both only in part1
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(4, 2))),
      StructType(Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Version", IntegerType, nullable = false)
      ))
    )

    val result = index.join(queryData, Seq("Id", "Version"), "inner")
    val resultRows = result.collect()
    resultRows.length should be > 0
    // All results should come from part1 data (Id=4, Version=2, Value=9.0)
    resultRows.foreach { row =>
      row.getAs[Int]("Id") shouldBe 4
      row.getAs[Int]("Version") shouldBe 2
    }
  }

  test("should handle single column query unchanged") {
    val index = Index("intersect_single", testSchema, "csv", Map("header" -> "true"))

    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    // Id=4 is only in part1
    val files = index.locateFiles(Map("Id" -> Array(4)))
    files should have size 1
    files should contain(path1)
  }
}
