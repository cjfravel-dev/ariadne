package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.Index.DataFrameOps
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests pinning the invariant that a join result schema does not depend on whether any file matched.
 *
 * The empty-result branch of `joinDf` used to build its DataFrame from the full stored schema, ignoring any active
 * `select()`. That made the output schema data-dependent: the same code returned different columns depending only on
 * whether a key happened to match.
 */
class EmptyResultSchemaTests extends SparkTests with Matchers {

  private val temporalSchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Status", StringType, nullable = true),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true)))

  /**
   * Writes a parquet file with Ids 1 and 2 so that a query for Id=999 matches no files.
   */
  private def writeDataFile(name: String): String = {
    val rows =
      Seq(
        Row(1, "active", 10.0, java.sql.Timestamp.valueOf("2024-01-15 00:00:00")),
        Row(2, "closed", 20.0, java.sql.Timestamp.valueOf("2024-01-15 00:00:00")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), temporalSchema)
    val path = s"${System.getProperty("java.io.tmpdir")}/ariadne-empty-schema-$name"
    df.write.mode("overwrite").parquet(path)
    path
  }

  private def queryFor(ids: Seq[Int]) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(ids.map(Row(_))),
      StructType(Seq(StructField("Id", IntegerType, nullable = false))))

  test("join result schema is identical whether or not any file matches") {
    val path = writeDataFile("temporal")
    val index = Index("empty_schema_temporal", temporalSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    index.select("Id", "Status")

    val matched = index.join(queryFor(Seq(1, 2)), Seq("Id"), "inner")
    val unmatched = index.join(queryFor(Seq(999)), Seq("Id"), "inner")

    // The no-match branch used to emit the full stored schema, including Value and UpdatedAt.
    unmatched.schema.fieldNames.toSeq should contain theSameElementsInOrderAs matched.schema.fieldNames.toSeq
    unmatched.count() should be(0)
  }

  test("join result schema is identical in the DataFrame-left direction") {
    val path = writeDataFile("dfleft")
    val index = Index("empty_schema_df_left", temporalSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addIndex("Id")
    index.update

    index.select("Id", "Status")

    val matched = queryFor(Seq(1)).join(index, Seq("Id"), "inner")
    val unmatched = queryFor(Seq(999)).join(index, Seq("Id"), "inner")

    unmatched.schema.fieldNames.toSeq should contain theSameElementsInOrderAs matched.schema.fieldNames.toSeq
  }

  test("outer join does not null-pad unselected columns onto valid left rows") {
    val path = writeDataFile("outer")
    val index = Index("empty_schema_outer", temporalSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addIndex("Id")
    index.update

    index.select("Id", "Status")

    // No file matches, but a left outer join still returns the left rows. The widened schema
    // therefore showed up on a result that was not empty at all.
    val result = queryFor(Seq(999)).join(index, Seq("Id"), "left")

    result.schema.fieldNames.toSeq should contain theSameElementsInOrderAs Seq("Id", "Status")
    result.count() should be(1)
  }

  test("no selection still returns the full stored schema when nothing matches") {
    val path = writeDataFile("noselect")
    val index = Index("empty_schema_no_selection", temporalSchema, "parquet")
    index.addFile(s"$path/*.parquet")
    index.addIndex("Id")
    index.update

    val matched = index.join(queryFor(Seq(1)), Seq("Id"), "inner")
    val unmatched = index.join(queryFor(Seq(999)), Seq("Id"), "inner")

    unmatched.schema.fieldNames.toSeq should contain theSameElementsInOrderAs matched.schema.fieldNames.toSeq
    unmatched.schema.fieldNames.toSeq should contain("UpdatedAt")
  }
}
