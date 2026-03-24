package dev.cjfravel.ariadne

import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/** Tests for intersection of heterogeneous index types (regular and bloom filter),
  * verifying AND semantics when locating files across mixed index columns.
  */
class MixedIndexIntersectionTests extends SparkTests with Matchers {

  val testSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("category", StringType, nullable = false),
      StructField("value", DoubleType, nullable = false)
    )
  )

  test("should intersect regular and bloom indexes correctly (AND semantics)") {
    val indexName = "mixed_index_intersect"
    val index = Index(indexName, testSchema, "csv", Map("header" -> "true"))
    
    // Create dummy data
    val _spark = spark
    import _spark.implicits._
    val data1 = Seq((1, "A", 10.0), (2, "B", 20.0)).toDF("id", "category", "value")
    val data2 = Seq((3, "C", 30.0), (4, "D", 40.0)).toDF("id", "category", "value")
    
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
    
    // Query: id=1 (present in path1) AND category="Z" (not present in any file)
    // Expected: Empty set
    // Bug: If bloom filter returns empty set (no match), it might be ignored, and regular index (id=1) returns path1.
    
    // We need to use locateFilesFromDataFrame logic, which is used by join() or can be called directly if we expose it or use locateFiles
    // locateFiles maps to locateFilesFromDataFrame internally? No, locateFiles maps to locateFilesRegular usually.
    // Let's check Index.scala locateFiles.
    
    val queryDf = Seq((1, "Z")).toDF("id", "category")
    
    // Using join to trigger locateFilesFromDataFrame
    val result = index.join(queryDf, Seq("id", "category"), "inner")
    
    // If the bug exists, result will contain rows from path1 because id=1 matches path1
    // and the bloom filter constraint (category="Z") returning 0 files is ignored.
    
    val rows = result.collect()
    rows.length shouldBe 0
  }
}
