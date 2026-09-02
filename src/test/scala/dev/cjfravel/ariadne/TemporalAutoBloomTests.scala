package dev.cjfravel.ariadne

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests for auto-bloom filtering on temporal index columns.
 *
 * Temporal columns are the likeliest to exceed `largeIndexLimit` — they typically hold high-cardinality entity ids — so
 * they benefit most from skipping `large_indexes/` at query time. Unlike other index types they store `(value, max_ts)`
 * structs, and their file location is a global argmax over `max_ts` rather than a plain membership test, so both the
 * build and query sides are covered here.
 *
 * temporal_part0.csv: Id=1,2,3,4 all at 2024-01-15. temporal_part1.csv: Id=1 at 2024-06, Id=2 at 2024-03, Id=5 at
 * 2024-06. Latest version therefore lives in part1 for Id=1,2,5 and in part0 for Id=3,4.
 */
class TemporalAutoBloomTests extends SparkTests with Matchers {

  val temporalSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true)))

  private def readMetadata(index: Index): IndexMetadata = {
    val metadataPath = new Path(index.storagePath, "metadata.json")
    val stream = index.open(metadataPath)
    try {
      val bytes = new Array[Byte](stream.available())
      stream.readFully(bytes)
      IndexMetadata(new String(bytes, StandardCharsets.UTF_8))
    } finally {
      stream.close()
    }
  }

  private def indexTable(index: Index): DataFrame =
    spark.read.format("delta").load(new Path(index.storagePath, "index").toString)

  /**
   * Builds a temporal index over both fixture files.
   *
   * @param name
   *   index name
   * @param largeIndexLimit
   *   value for `spark.ariadne.largeIndexLimit` during the update; `1` forces every file into the large index and so
   *   turns auto-bloom on, while the default leaves everything inline with no auto-bloom
   */
  private def buildTemporalIndex(name: String, largeIndexLimit: String): Index = {
    spark.conf.set("spark.ariadne.largeIndexLimit", largeIndexLimit)
    try {
      val index = Index(name, temporalSchema, "csv", Map("header" -> "true"))
      index.addFile(resourcePath("/data/temporal_part0.csv"), resourcePath("/data/temporal_part1.csv"))
      index.addTemporalIndex("Id", "UpdatedAt")
      index.update
      index
    } finally {
      spark.conf.set("spark.ariadne.largeIndexLimit", "500000")
    }
  }

  test("should automatically create an auto-bloom filter for large temporal columns") {
    val index = buildTemporalIndex("temporal_auto_bloom_create", "1")

    indexTable(index).columns should contain("auto_bloom_Id")
    readMetadata(index).auto_bloom_indexes.asScala should contain("Id")
  }

  test("should build the temporal auto-bloom filter over values rather than over (value, max_ts) structs") {
    val index = buildTemporalIndex("temporal_auto_bloom_value_field", "1")
    val df = indexTable(index)

    // The decisive check for temporal auto-bloom. columnValueRows yields (filename, struct(value, max_ts))
    // for temporal columns, so folding the filter over the struct rather than its `value` field would hash a
    // representation no scalar probe can ever reproduce, and every probe below would come back empty.
    // Id=3 lives only in part0, so only part0 may be a candidate.
    val onlyInPart0 = index.getAutoBloomCandidates("Id", Array(3), df)
    onlyInPart0 shouldBe defined
    onlyInPart0.get.size shouldBe 1
    onlyInPart0.get.head should include("temporal_part0.csv")

    // Id=5 lives only in part1.
    val onlyInPart1 = index.getAutoBloomCandidates("Id", Array(5), df)
    onlyInPart1 shouldBe defined
    onlyInPart1.get.size shouldBe 1
    onlyInPart1.get.head should include("temporal_part1.csv")

    // Id=1 lives in both, so neither file may be pruned even though only part1 holds the latest version.
    val inBoth = index.getAutoBloomCandidates("Id", Array(1), df)
    inBoth shouldBe defined
    inBoth.get.size shouldBe 2
  }

  test("should keep latest-version semantics when the auto-bloom pre-filter prunes files") {
    val index = buildTemporalIndex("temporal_auto_bloom_latest", "1")

    // Id=1 is in both files; only the file holding the newest version may be returned.
    val files1 = index.locateFiles(Map("Id" -> Array(1)))
    files1.size shouldBe 1
    files1.head should include("temporal_part1.csv")

    // Id=3 is only in part0.
    val files3 = index.locateFiles(Map("Id" -> Array(3)))
    files3.size shouldBe 1
    files3.head should include("temporal_part0.csv")

    // Id=1 resolves to part1 and Id=3 to part0, so both files are needed.
    index.locateFiles(Map("Id" -> Array(1, 3))).size shouldBe 2
  }

  test("should locate identical files with and without the auto-bloom pre-filter") {
    val withBloom = buildTemporalIndex("temporal_auto_bloom_parity_on", "1")
    val withoutBloom = buildTemporalIndex("temporal_auto_bloom_parity_off", "500000")

    readMetadata(withBloom).auto_bloom_indexes.asScala should contain("Id")
    readMetadata(withoutBloom).auto_bloom_indexes.asScala shouldBe empty

    // Pruning is an optimization: it must never change which files a query resolves to.
    Seq(Array[Any](1), Array[Any](2), Array[Any](3), Array[Any](4), Array[Any](5), Array[Any](1, 2, 3, 4, 5))
      .foreach { values =>
        val pruned = withBloom.locateFiles(Map("Id" -> values))
        val unpruned = withoutBloom.locateFiles(Map("Id" -> values))
        withClue(s"Id in ${values.mkString(", ")}: ") {
          pruned.map(f => new Path(f).getName) shouldBe unpruned.map(f => new Path(f).getName)
        }
      }
  }

  test("should return only the latest version per entity when joining through the auto-bloom pre-filter") {
    val index = buildTemporalIndex("temporal_auto_bloom_join", "1")

    val _spark = spark
    import _spark.implicits._
    val queryDf = Seq(1, 2, 3).toDF("Id")

    val result = index.join(queryDf, Seq("Id"), "inner")
    val values = result.select("Id", "Value").collect().map(r => r.getInt(0) -> r.getDouble(1)).toMap

    // Latest: Id=1 -> 150.0 (part1), Id=2 -> 250.0 (part1), Id=3 -> 300.0 (part0).
    values shouldBe Map(1 -> 150.0, 2 -> 250.0, 3 -> 300.0)
  }

  test("should skip the temporal pre-filter when the query value set exceeds the derived bound") {
    val index = buildTemporalIndex("temporal_auto_bloom_bound", "1")

    val _spark = spark
    import _spark.implicits._
    // Past the bound derived from the auto-bloom FPR the pre-filter is skipped entirely, which
    // must cost work rather than rows.
    val overBound = BloomFilterOperations.maxProbeValues(index.autoBloomFpr) + 10
    val queryDf = Seq.range(1, overBound).toDF("Id")

    val result = index.join(queryDf, Seq("Id"), "inner")
    val values = result.select("Id", "Value").collect().map(r => r.getInt(0) -> r.getDouble(1)).toMap

    values shouldBe Map(1 -> 150.0, 2 -> 250.0, 3 -> 300.0, 4 -> 400.0, 5 -> 500.0)
  }

  test("should not build an auto-bloom filter for temporal columns under the limit") {
    val index = buildTemporalIndex("temporal_auto_bloom_under_limit", "500000")

    readMetadata(index).auto_bloom_indexes.asScala shouldBe empty
    indexTable(index).columns should not contain "auto_bloom_Id"

    val files = index.locateFiles(Map("Id" -> Array(1)))
    files.size shouldBe 1
    files.head should include("temporal_part1.csv")
  }
}
