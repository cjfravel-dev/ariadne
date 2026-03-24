package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.spark.sql.types._

/** Tests for [[IndexCatalog]] covering discovery, inspection, retrieval,
  * DataFrame conversion, and removal of indexes from the catalog.
  */
class IndexCatalogTests extends SparkTests {

  val schema1 = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val schema2 = StructType(
    Seq(
      StructField("UserId", StringType, nullable = false),
      StructField("Status", StringType, nullable = false),
      StructField("UpdatedAt", TimestampType, nullable = false)
    )
  )

  val schemaWithArray = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField(
        "Tags",
        ArrayType(
          StructType(
            Seq(
              StructField("name", StringType, nullable = false)
            )
          )
        ),
        nullable = false
      )
    )
  )

  // --- list ---

  test("list returns empty when no indexes exist") {
    val names = IndexCatalog.list()
    assert(names.isEmpty)
  }

  test("list returns index names after creation") {
    Index("catalog_list_a", schema1, "parquet")
    Index("catalog_list_b", schema1, "parquet")
    Index("catalog_list_c", schema1, "parquet")

    val names = IndexCatalog.list()
    assert(names.contains("catalog_list_a"))
    assert(names.contains("catalog_list_b"))
    assert(names.contains("catalog_list_c"))
  }

  test("list returns names in sorted order") {
    Index("catalog_sorted_z", schema1, "parquet")
    Index("catalog_sorted_a", schema1, "parquet")
    Index("catalog_sorted_m", schema1, "parquet")

    val names = IndexCatalog.list()
    val relevant = names.filter(_.startsWith("catalog_sorted_"))
    assert(relevant == relevant.sorted)
  }

  test("list does not include removed indexes") {
    Index("catalog_remove_list", schema1, "parquet")
    assert(IndexCatalog.list().contains("catalog_remove_list"))

    IndexCatalog.remove("catalog_remove_list")
    assert(!IndexCatalog.list().contains("catalog_remove_list"))
  }

  // --- exists ---

  test("exists returns false for non-existent index") {
    assert(!IndexCatalog.exists("does_not_exist"))
  }

  test("exists returns true for existing index") {
    Index("catalog_exists", schema1, "parquet")
    assert(IndexCatalog.exists("catalog_exists"))
  }

  // --- describe ---

  test("describe throws IndexNotFoundException for missing index") {
    assertThrows[IndexNotFoundException] {
      IndexCatalog.describe("nonexistent_index")
    }
  }

  test("describe returns correct summary for index with regular indexes") {
    val index = Index("catalog_desc_regular", schema1, "parquet")
    index.addIndex("Id")
    index.addIndex("Version")

    val summary = IndexCatalog.describe("catalog_desc_regular")
    assert(summary.name == "catalog_desc_regular")
    assert(summary.format == "parquet")
    assert(summary.regularIndexes == Set("Id", "Version"))
    assert(summary.bloomIndexes.isEmpty)
    assert(summary.computedIndexes.isEmpty)
    assert(summary.temporalIndexes.isEmpty)
    assert(summary.rangeIndexes.isEmpty)
    assert(summary.explodedFieldIndexes.isEmpty)
    assert(summary.columns == Set("Id", "Version"))
  }

  test("describe returns correct summary for index with bloom indexes") {
    val index = Index("catalog_desc_bloom", schema1, "parquet")
    index.addBloomIndex("Id", 0.05)

    val summary = IndexCatalog.describe("catalog_desc_bloom")
    assert(summary.bloomIndexes == Set("Id"))
    assert(summary.regularIndexes.isEmpty)
    assert(summary.columns.contains("Id"))
  }

  test("describe returns correct summary for index with computed indexes") {
    val index = Index("catalog_desc_computed", schema1, "parquet")
    index.addComputedIndex("category", "substring(cast(Id as string), 1, 2)")

    val summary = IndexCatalog.describe("catalog_desc_computed")
    assert(summary.computedIndexes == Set("category"))
    assert(summary.columns.contains("category"))
  }

  test("describe returns correct summary for index with temporal indexes") {
    val index = Index("catalog_desc_temporal", schema2, "parquet")
    index.addTemporalIndex("UserId", "UpdatedAt")

    val summary = IndexCatalog.describe("catalog_desc_temporal")
    assert(summary.temporalIndexes == Set("UserId"))
    assert(summary.columns.contains("UserId"))
  }

  test("describe returns correct summary for index with range indexes") {
    val index = Index("catalog_desc_range", schema1, "parquet")
    index.addRangeIndex("Version")

    val summary = IndexCatalog.describe("catalog_desc_range")
    assert(summary.rangeIndexes == Set("Version"))
    assert(summary.columns.contains("Version"))
  }

  test("describe returns correct summary for index with exploded field indexes") {
    val index = Index("catalog_desc_exploded", schemaWithArray, "parquet")
    index.addExplodedFieldIndex("Tags", "name", "tag_name")

    val summary = IndexCatalog.describe("catalog_desc_exploded")
    assert(summary.explodedFieldIndexes == Set("tag_name"))
    assert(summary.columns.contains("tag_name"))
  }

  test("describe returns correct summary for index with mixed index types") {
    val index = Index("catalog_desc_mixed", schema1, "parquet")
    index.addIndex("Id")
    index.addRangeIndex("Version")
    index.addComputedIndex("cat", "substring(cast(Id as string), 1, 2)")

    val summary = IndexCatalog.describe("catalog_desc_mixed")
    assert(summary.regularIndexes == Set("Id"))
    assert(summary.rangeIndexes == Set("Version"))
    assert(summary.computedIndexes == Set("cat"))
    assert(summary.columns == Set("Id", "Version", "cat"))
  }

  test("describe returns correct file count with tracked files") {
    val index = Index("catalog_desc_files", schema1, "parquet")
    index.addIndex("Id")
    index.addFile(resourcePath("/data/table1_part0.csv"))

    val summary = IndexCatalog.describe("catalog_desc_files")
    assert(summary.fileCount == 1)
  }

  test("describe returns zero file count for index with no files") {
    val index = Index("catalog_desc_no_files", schema1, "parquet")
    index.addIndex("Id")

    val summary = IndexCatalog.describe("catalog_desc_no_files")
    assert(summary.fileCount == 0)
  }

  test("describe returns totalIndexedFileSize from metadata") {
    val index = Index("catalog_desc_size", schema1, "parquet")
    index.addIndex("Id")

    val summary = IndexCatalog.describe("catalog_desc_size")
    assert(summary.totalIndexedFileSize == -1L)
  }

  test("describe reflects index modifications") {
    val index = Index("catalog_modify", schema1, "parquet")
    index.addIndex("Id")

    val summary1 = IndexCatalog.describe("catalog_modify")
    assert(summary1.regularIndexes == Set("Id"))
    assert(summary1.rangeIndexes.isEmpty)

    index.addRangeIndex("Version")

    val summary2 = IndexCatalog.describe("catalog_modify")
    assert(summary2.regularIndexes == Set("Id"))
    assert(summary2.rangeIndexes == Set("Version"))
    assert(summary2.columns == Set("Id", "Version"))
  }

  // --- describeAll ---

  test("describeAll returns summaries for all indexes") {
    Index("catalog_all_1", schema1, "parquet")
    Index("catalog_all_2", schema1, "parquet")

    val summaries = IndexCatalog.describeAll()
    val names = summaries.map(_.name)
    assert(names.contains("catalog_all_1"))
    assert(names.contains("catalog_all_2"))
  }

  // --- get ---

  test("get throws IndexNotFoundException for missing index") {
    assertThrows[IndexNotFoundException] {
      IndexCatalog.get("nonexistent_get")
    }
  }

  test("get returns a functional Index instance") {
    val original = Index("catalog_get_test", schema1, "parquet")
    original.addIndex("Id")

    val fetched = IndexCatalog.get("catalog_get_test")
    assert(fetched.name == "catalog_get_test")
    assert(fetched.format == "parquet")
    assert(fetched.indexes.contains("Id"))
  }

  // --- findIndexes ---

  test("findIndexes returns empty for file not tracked by any index") {
    Index("catalog_find_none", schema1, "parquet")
    val result = IndexCatalog.findIndexes("nonexistent/file.parquet")
    assert(result.isEmpty)
  }

  test("findIndexes returns indexes that track a given file") {
    val file = resourcePath("/data/table1_part0.csv")

    val idx1 = Index("catalog_find_a", schema1, "parquet")
    idx1.addIndex("Id")
    idx1.addFile(file)

    val idx2 = Index("catalog_find_b", schema1, "parquet")
    idx2.addIndex("Id")
    idx2.addFile(file)

    val idx3 = Index("catalog_find_c", schema1, "parquet")
    idx3.addIndex("Id")
    // idx3 does NOT have the file

    val result = IndexCatalog.findIndexes(file)
    assert(result.contains("catalog_find_a"))
    assert(result.contains("catalog_find_b"))
    assert(!result.contains("catalog_find_c"))
  }

  test("findIndexes returns results in sorted order") {
    val file = resourcePath("/data/table1_part1.csv")

    val idxZ = Index("catalog_find_z", schema1, "parquet")
    idxZ.addFile(file)
    val idxA = Index("catalog_find_aa", schema1, "parquet")
    idxA.addFile(file)

    val result = IndexCatalog.findIndexes(file)
    val relevant = result.filter(_.startsWith("catalog_find_a"))
    assert(relevant.nonEmpty)
    assert(result == result.sorted)
  }

  // --- toDF ---

  test("toDF returns correct schema") {
    Index("catalog_df_schema", schema1, "parquet")

    val df = IndexCatalog.toDF()
    val expectedColumns = Set(
      "name", "format", "regular_indexes", "bloom_indexes",
      "computed_indexes", "temporal_indexes", "range_indexes",
      "exploded_field_indexes", "file_count", "total_indexed_file_size"
    )
    assert(df.columns.toSet == expectedColumns)
  }

  test("toDF includes correct data for indexes") {
    val index = Index("catalog_df_data", schema1, "parquet")
    index.addIndex("Id")
    index.addRangeIndex("Version")

    val df = IndexCatalog.toDF()
    val row = df.filter(df("name") === "catalog_df_data").collect()
    assert(row.length == 1)
    assert(row(0).getAs[String]("format") == "parquet")
    assert(row(0).getAs[String]("regular_indexes").contains("Id"))
    assert(row(0).getAs[String]("range_indexes").contains("Version"))
  }

  test("toDF returns one row per index") {
    Index("catalog_df_count_a", schema1, "parquet")
    Index("catalog_df_count_b", schema1, "parquet")

    val df = IndexCatalog.toDF()
    val relevantRows = df.filter(df("name").startsWith("catalog_df_count_")).collect()
    assert(relevantRows.length == 2)
  }

  // --- remove ---

  test("remove removes an existing index") {
    Index("catalog_remove", schema1, "parquet")
    assert(IndexCatalog.exists("catalog_remove"))

    IndexCatalog.remove("catalog_remove")
    assert(!IndexCatalog.exists("catalog_remove"))
  }

  test("remove throws IndexNotFoundException for missing index") {
    assertThrows[IndexNotFoundException] {
      IndexCatalog.remove("nonexistent_remove")
    }
  }
}
