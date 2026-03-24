package dev.cjfravel.ariadne

import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import java.nio.charset.StandardCharsets

/** Tests for file size tracking, verifying that `file_size` is stored in the
  * index table during `update` and accessible via index metadata.
  */
class FileSizeTrackingTests extends SparkTests with Matchers {

  val table1Schema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  /** Reads metadata from disk for the given index name. */
  private def readMetadataFromDisk(indexName: String): IndexMetadata = {
    val metadataPath = new org.apache.hadoop.fs.Path(
      tempDir.toString + "/indexes/" + indexName + "/metadata.json"
    )
    val fs = metadataPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val inputStream = fs.open(metadataPath)
    val jsonString = Source.fromInputStream(inputStream)(StandardCharsets.UTF_8).mkString
    inputStream.close()
    IndexMetadata(jsonString)
  }

  /** Writes metadata to disk for the given index name. */
  private def writeMetadataToDisk(indexName: String, metadata: IndexMetadata): Unit = {
    val metadataPath = new org.apache.hadoop.fs.Path(
      tempDir.toString + "/indexes/" + indexName + "/metadata.json"
    )
    val fs = metadataPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val jsonString = new com.google.gson.Gson().toJson(metadata)
    val outputStream = fs.create(metadataPath, true)
    outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
    outputStream.flush()
    outputStream.close()
  }

  /** Returns the path to the index Delta table. */
  private def indexPath(indexName: String): String =
    tempDir.toString + "/indexes/" + indexName + "/index"

  test("should store file_size in index table during update") {
    val csvOptions = Map("header" -> "true")
    val index = Index("filesize_store", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    val indexDf = spark.read.format("delta").load(indexPath("filesize_store"))
    indexDf.columns should contain("file_size")

    val fileSizes = indexDf.select("filename", "file_size").collect()
    fileSizes.foreach { row =>
      val fileSize = row.getLong(1)
      fileSize should be > 0L
    }
  }

  test("should track total_indexed_file_size in metadata") {
    val csvOptions = Map("header" -> "true")
    val index = Index("filesize_total", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    val metadata = readMetadataFromDisk("filesize_total")
    val total = metadata.total_indexed_file_size.toLong
    total should be > 0L

    // Verify total equals sum of individual file_size values
    val indexDf = spark.read.format("delta").load(indexPath("filesize_total"))
    val sumResult = indexDf.agg(sum("file_size")).head()
    val sumOfSizes = sumResult.getLong(0)
    total shouldBe sumOfSizes
  }

  test("should decrement total on deleteFiles") {
    val csvOptions = Map("header" -> "true")
    val index = Index("filesize_delete", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    val path1 = resourcePath("/data/table1_part1.csv")
    index.addFile(path0, path1)
    index.addIndex("Id")
    index.update

    val metadataBefore = readMetadataFromDisk("filesize_delete")
    val totalBefore = metadataBefore.total_indexed_file_size.toLong
    totalBefore should be > 0L

    // Get size of file being deleted
    val indexDf = spark.read.format("delta").load(indexPath("filesize_delete"))
    val file0Size = indexDf
      .where(col("filename") === path0)
      .select("file_size")
      .head()
      .getLong(0)
    file0Size should be > 0L

    index.deleteFiles(path0)

    val metadataAfter = readMetadataFromDisk("filesize_delete")
    val totalAfter = metadataAfter.total_indexed_file_size.toLong
    totalAfter shouldBe (totalBefore - file0Size)
    totalAfter should be > 0L
  }

  test("should backfill file_size for existing index rows") {
    val csvOptions = Map("header" -> "true")
    val index = Index("filesize_backfill", table1Schema, "csv", csvOptions)
    val path0 = resourcePath("/data/table1_part0.csv")
    index.addFile(path0)
    index.addIndex("Id")
    index.update

    // Verify file_size exists
    val indexDf = spark.read.format("delta").load(indexPath("filesize_backfill"))
    indexDf.columns should contain("file_size")
    val originalSize = indexDf.select("file_size").head().getLong(0)
    originalSize should be > 0L

    // Simulate old index by nulling out file_size via Delta merge
    import io.delta.tables.DeltaTable
    val dt = DeltaTable.forPath(spark, indexPath("filesize_backfill"))
    val allFiles = spark.read.format("delta").load(indexPath("filesize_backfill"))
      .select("filename").distinct()
    dt.as("target")
      .merge(allFiles.as("source"), "target.filename = source.filename")
      .whenMatched()
      .update(Map("file_size" -> lit(null).cast("long")))
      .execute()

    // Also reset metadata to trigger recalculation
    val metadata = readMetadataFromDisk("filesize_backfill")
    metadata.total_indexed_file_size = -1L
    writeMetadataToDisk("filesize_backfill", metadata)

    // Verify file_size is null now
    val nullDf = spark.read.format("delta").load(indexPath("filesize_backfill"))
    val nullCount = nullDf.where(col("file_size").isNull).count()
    nullCount should be > 0L

    // Reload index and call update - should trigger backfill
    val reloadedIndex = Index("filesize_backfill", table1Schema, "csv", csvOptions)
    reloadedIndex.update

    // Verify file_size is populated again
    val backfilledDf = spark.read.format("delta").load(indexPath("filesize_backfill"))
    val backfilledSize = backfilledDf.select("file_size").head().getLong(0)
    backfilledSize shouldBe originalSize

    // Verify metadata total is recalculated
    val finalMetadata = readMetadataFromDisk("filesize_backfill")
    finalMetadata.total_indexed_file_size.toLong should be > 0L
  }
}
