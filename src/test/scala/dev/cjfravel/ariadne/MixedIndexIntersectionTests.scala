package dev.cjfravel.ariadne

import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests for intersection of heterogeneous index types (regular and bloom filter), verifying AND semantics when locating
 * files across mixed index columns.
 */
class MixedIndexIntersectionTests extends SparkTests with Matchers {

  val testSchema =
    StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false)))

  test("should intersect regular and bloom indexes correctly (AND semantics)") {
    val indexName = "mixed_index_intersect"
    val index = Index(indexName, testSchema, "csv", Map("header" -> "true"))

    // Create dummy data
    val _spark = spark
    import _spark.implicits._
    val data1 =
      Seq((1, "A", 10.0), (2, "B", 20.0)).toDF("id", "category", "value")
    val data2 =
      Seq((3, "C", 30.0), (4, "D", 40.0)).toDF("id", "category", "value")

    val path1 = s"$tempDir/data1.csv"
    val path2 = s"$tempDir/data2.csv"

    data1.write.option("header", "true").csv(path1)
    data2.write.option("header", "true").csv(path2)

    index.addFile(path1, path2)

    // category as bloom index
    index.addBloomIndex("category", 0.01)
    // id as regular index
    index.addIndex("id")

    index.update

    // id=1 is present in path1 but category="Z" is present in no file, so the intersection of the
    // regular and bloom index results must be empty.
    val queryDf = Seq((1, "Z")).toDF("id", "category")

    // join() routes through locateFilesFromDataFrame, which intersects results across index types.
    val result = index.join(queryDf, Seq("id", "category"), "inner")

    // A bloom result of zero files must not be discarded in favour of the regular index match on id=1.
    val rows = result.collect()
    rows.length shouldBe 0
  }
}
