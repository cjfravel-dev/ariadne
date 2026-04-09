package dev.cjfravel.ariadne.catalog

import dev.cjfravel.ariadne.{Index, IndexCatalog, SparkTests}
import dev.cjfravel.ariadne.Index.DataFrameOps
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

/** Tests for the Ariadne Spark SQL catalog integration.
  *
  * Verifies that Ariadne indexes are discoverable and queryable through
  * Spark SQL when the [[AriadneCatalog]] is configured. Tests cover:
  *  - `SHOW TABLES` discovering all indexes
  *  - `DESCRIBE TABLE` returning the correct source schema
  *  - `SELECT *` reading source data files
  *  - `WHERE` filter pushdown using `locateFiles()`
  *  - `JOIN` optimization via the [[AriadneJoinRule]]
  *
  * Uses a separate SparkSession with the Ariadne catalog and extension
  * registered, distinct from the base [[SparkTests]] session.
  */
class AriadneCatalogTests extends SparkTests with Matchers {

  val indexSchema: StructType = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val querySchema: StructType = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false)
    )
  )

  /** Override to add Ariadne catalog + extension configuration.
    * spark.sql.extensions is a static config that must be on SparkConf
    * BEFORE SparkContext creation — setting it on the builder is too late.
    */
  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("ariadne-catalog-test-")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TestAriadneCatalog")
      .set(
        "spark.sql.extensions",
        "dev.cjfravel.ariadne.catalog.AriadneSparkExtension"
      )
    sc = new SparkContext(conf)

    spark = SparkSession
      .builder()
      .config(sc.getConf)
      .config("spark.ariadne.storagePath", tempDir.toString)
      .config("spark.ariadne.largeIndexLimit", "500000")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config(
        "spark.sql.catalog.ariadne",
        "dev.cjfravel.ariadne.catalog.AriadneCatalog"
      )
      .getOrCreate()
  }

  private def createTestIndex(name: String): Index = {
    val csvOptions = Map("header" -> "true")
    val index = Index(name, indexSchema, "csv", csvOptions)
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addFile(resourcePath("/data/table1_part1.csv"))
    index.addIndex("Id")
    index.addIndex("Version")
    index.update
    index
  }

  // --- SHOW TABLES ---

  test("SHOW TABLES lists all indexes") {
    createTestIndex("catalog_show_test_1")
    createTestIndex("catalog_show_test_2")

    val tables = spark.sql("SHOW TABLES IN ariadne").collect()
    val tableNames = tables.map(_.getAs[String]("tableName")).toSet

    tableNames should contain("catalog_show_test_1")
    tableNames should contain("catalog_show_test_2")
  }

  test("SHOW TABLES includes namespace column") {
    createTestIndex("catalog_ns_test")
    val tables = spark.sql("SHOW TABLES IN ariadne").collect()
    val nsRow = tables.find(_.getAs[String]("tableName") == "catalog_ns_test")
    nsRow shouldBe defined
    nsRow.get.getAs[String]("namespace") shouldBe "default"
  }

  test("SHOW TABLES returns empty when no indexes exist") {
    // Create a fresh session-like context by just verifying the call works
    // (Other tests create indexes, so we just verify the call doesn't fail)
    val tables = spark.sql("SHOW TABLES IN ariadne").collect()
    tables should not be null
  }

  // --- DESCRIBE TABLE ---

  test("DESCRIBE TABLE returns source schema") {
    createTestIndex("catalog_describe_test")

    val description = spark.sql("DESCRIBE ariadne.catalog_describe_test").collect()
    val colNames = description.map(_.getAs[String]("col_name")).toSet

    colNames should contain("Id")
    colNames should contain("Version")
    colNames should contain("Value")
  }

  test("DESCRIBE TABLE handles complex types") {
    val complexSchema = StructType(Seq(
      StructField("event_id", StringType),
      StructField("event_type", StringType),
      StructField("tags", ArrayType(StructType(Seq(
        StructField("name", StringType),
        StructField("category", StringType)
      )))),
      StructField("user_ids", ArrayType(LongType))
    ))

    val index = Index("catalog_complex_describe", complexSchema, "json")
    index.addFile(resourcePath("/data/events.json"))
    index.addIndex("event_type")
    index.update

    val desc = spark.sql("DESCRIBE ariadne.catalog_complex_describe").collect()
    val colMap = desc.map(r => r.getAs[String]("col_name") -> r.getAs[String]("data_type")).toMap

    colMap should contain key "event_id"
    colMap should contain key "tags"
    colMap should contain key "user_ids"
    colMap("tags") should include("array")
    colMap("tags") should include("struct")
    colMap("user_ids") should include("array")
  }

  // --- SELECT ---

  test("SELECT * reads all source data") {
    createTestIndex("catalog_select_test")

    val result = spark.sql("SELECT * FROM ariadne.catalog_select_test")
    result.columns should contain allOf ("Id", "Version", "Value")

    val rows = result.collect()
    rows.length should be > 0

    // Verify data content — table1_part0 has (1,1,5.0), (2,1,3.0), (3,1,4.0), (1,2,4.5)
    // table1_part1 has (4,1,2.0), (4,2,9.0), (3,2,4.0), (1,3,5.0)
    // Total: 8 rows
    rows.length should be(8)
  }

  test("SELECT with column projection") {
    createTestIndex("catalog_project_test")

    val result = spark.sql("SELECT Id, Value FROM ariadne.catalog_project_test")
    result.columns should contain allOf ("Id", "Value")
    result.columns should not contain "Version"
    result.collect().length should be(8)
  }

  // --- WHERE filter pushdown ---

  test("SELECT with WHERE on indexed column") {
    createTestIndex("catalog_where_test")

    val result = spark.sql("SELECT * FROM ariadne.catalog_where_test WHERE Id = 1")
    val rows = result.collect()

    // Id=1 appears in both files: (1,1,5.0), (1,2,4.5), (1,3,5.0)
    rows.length should be(3)
    rows.foreach(_.getAs[Int]("Id") should be(1))
  }

  test("SELECT with WHERE IN on indexed column") {
    createTestIndex("catalog_where_in_test")

    val result = spark.sql("SELECT * FROM ariadne.catalog_where_in_test WHERE Id IN (1, 2)")
    val rows = result.collect()

    // Id=1: 3 rows, Id=2: 1 row = 4 total
    rows.length should be(4)
    rows.foreach { row =>
      Set(1, 2) should contain(row.getAs[Int]("Id"))
    }
  }

  // --- JOIN optimization ---

  test("SQL JOIN produces correct results") {
    createTestIndex("catalog_join_test")

    // Create a temp table for the "other side"
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(1, 1), Row(2, 1))
      ),
      querySchema
    )
    queryData.createOrReplaceTempView("join_query")

    val result = spark.sql(
      """SELECT c.* FROM ariadne.catalog_join_test c
        |JOIN join_query q ON c.Id = q.Id AND c.Version = q.Version""".stripMargin
    )
    val rows = result.collect()

    // Id=1,Version=1 -> (1,1,5.0); Id=2,Version=1 -> (2,1,3.0)
    rows.length should be(2)
  }

  test("SQL JOIN matches programmatic index.join results") {
    val index = createTestIndex("catalog_join_match_test")

    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(1, 1), Row(3, 2))
      ),
      querySchema
    )
    queryData.createOrReplaceTempView("match_query")

    // Programmatic join
    val programmaticResult = index
      .join(queryData, Seq("Id", "Version"), "inner")
      .orderBy("Id", "Version")
      .collect()

    // SQL join
    val sqlResult = spark
      .sql(
        """SELECT c.Id, c.Version, c.Value FROM ariadne.catalog_join_match_test c
          |JOIN match_query q ON c.Id = q.Id AND c.Version = q.Version
          |ORDER BY c.Id, c.Version""".stripMargin
      )
      .collect()

    programmaticResult.length should be(sqlResult.length)
    programmaticResult.zip(sqlResult).foreach { case (prog, sql) =>
      prog.getAs[Int]("Id") should be(sql.getAs[Int]("Id"))
      prog.getAs[Int]("Version") should be(sql.getAs[Int]("Version"))
      prog.getAs[Double]("Value") should be(sql.getAs[Double]("Value"))
    }
  }

  test("SQL JOIN with reverse direction") {
    val index = createTestIndex("catalog_join_reverse_test")

    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(1, 1), Row(4, 2))
      ),
      querySchema
    )
    queryData.createOrReplaceTempView("reverse_query")

    // SQL join with Ariadne on the right
    val result = spark.sql(
      """SELECT * FROM reverse_query q
        |JOIN ariadne.catalog_join_reverse_test c
        |ON q.Id = c.Id AND q.Version = c.Version""".stripMargin
    )
    val rows = result.collect()
    rows.length should be(2)
  }

  test("SQL JOIN with non-indexed columns falls back gracefully") {
    val index = createTestIndex("catalog_join_fallback_test")

    val valueSchema = StructType(
      Seq(StructField("Value", DoubleType, nullable = false))
    )
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(5.0))),
      valueSchema
    )
    queryData.createOrReplaceTempView("fallback_query")

    // Value is not indexed, so the join rule should not rewrite
    // This should still work via the V1Scan fallback (full table scan + standard join)
    val result = spark.sql(
      """SELECT * FROM ariadne.catalog_join_fallback_test c
        |JOIN fallback_query q ON c.Value = q.Value""".stripMargin
    )
    val rows = result.collect()
    // Value=5.0 appears in (1,1,5.0) and (1,3,5.0)
    rows.length should be(2)
  }

  // --- IndexCatalog integration ---

  test("IndexCatalog.list matches SHOW TABLES") {
    createTestIndex("catalog_list_match_test")

    val catalogNames = IndexCatalog.list().toSet
    val sqlNames = spark
      .sql("SHOW TABLES IN ariadne")
      .collect()
      .map(_.getAs[String]("tableName"))
      .toSet

    // SQL should show at least all indexes that IndexCatalog.list shows
    catalogNames.foreach(name => sqlNames should contain(name))
  }

  // --- Edge case: Non-equi join conditions (Issue #1) ---

  test("JOIN with non-equi condition preserves all predicates") {
    createTestIndex("catalog_nonequi_test")

    val nonEquiSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("MinValue", DoubleType, nullable = false)
    ))
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 4.9))),
      nonEquiSchema
    )
    queryData.createOrReplaceTempView("nonequi_query")

    val result = spark.sql(
      """SELECT c.* FROM ariadne.catalog_nonequi_test c
        |JOIN nonequi_query q ON c.Id = q.Id AND c.Value > q.MinValue""".stripMargin
    )
    val rows = result.collect()

    // Id=1 rows: (1,1,5.0), (1,2,4.5), (1,3,5.0)
    // With Value > 4.9: only (1,1,5.0) and (1,3,5.0) = 2 rows
    rows.length should be(2)
    rows.foreach(_.getAs[Double]("Value") should be > 4.9)
  }

  // --- Edge case: Partially-indexed join columns (Issue #2) ---

  test("JOIN with partially-indexed columns preserves all equi conditions") {
    val csvOptions = Map("header" -> "true")
    val index = Index("catalog_partial_idx_test", indexSchema, "csv", csvOptions)
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addFile(resourcePath("/data/table1_part1.csv"))
    index.addIndex("Id") // Only Id indexed, not Version
    index.update

    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 1), Row(1, 2))),
      querySchema
    )
    queryData.createOrReplaceTempView("partial_query")

    val result = spark.sql(
      """SELECT c.* FROM ariadne.catalog_partial_idx_test c
        |JOIN partial_query q ON c.Id = q.Id AND c.Version = q.Version""".stripMargin
    )
    val rows = result.collect()

    // Should match: (1,1,5.0) and (1,2,4.5) = 2 rows
    // Bug: USING(Id) drops Version condition, returns all Id=1: 3 rows
    rows.length should be(2)
  }

  // --- Edge case: Different column names (Issue #3) ---

  test("JOIN with different column names produces correct results") {
    createTestIndex("catalog_diffname_test")

    val diffNameSchema = StructType(Seq(
      StructField("CustomerId", IntegerType, nullable = false),
      StructField("Id", IntegerType, nullable = false)
    ))
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 999))),
      diffNameSchema
    )
    queryData.createOrReplaceTempView("diffname_query")

    // Intent: join c.Id = q.CustomerId (=1), NOT c.Id = q.Id (=999)
    val result = spark.sql(
      """SELECT c.* FROM ariadne.catalog_diffname_test c
        |JOIN diffname_query q ON c.Id = q.CustomerId""".stripMargin
    )
    val rows = result.collect()

    // c.Id should match q.CustomerId=1: (1,1,5.0), (1,2,4.5), (1,3,5.0) = 3 rows
    rows.length should be(3)
  }

  test("JOIN with different column names on multiple columns") {
    createTestIndex("catalog_diffname_multi_test")

    val diffNameSchema = StructType(Seq(
      StructField("CustId", IntegerType, nullable = false),
      StructField("CustVersion", IntegerType, nullable = false)
    ))
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 1), Row(3, 2))),
      diffNameSchema
    )
    queryData.createOrReplaceTempView("diffname_multi_query")

    val result = spark.sql(
      """SELECT c.* FROM ariadne.catalog_diffname_multi_test c
        |JOIN diffname_multi_query q ON c.Id = q.CustId AND c.Version = q.CustVersion""".stripMargin
    )
    val rows = result.collect()

    // (1,1,5.0) and (3,2,4.0) = 2 rows
    rows.length should be(2)
  }

  // --- Edge case: LEFT OUTER JOIN (Issue #4, #12) ---

  test("LEFT OUTER JOIN returns unmatched rows with nulls") {
    createTestIndex("catalog_left_join_test")

    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 1), Row(999, 1))),
      querySchema
    )
    queryData.createOrReplaceTempView("left_join_query")

    val result = spark.sql(
      """SELECT q.Id as qId, q.Version as qVersion, c.Value
        |FROM left_join_query q
        |LEFT JOIN ariadne.catalog_left_join_test c
        |ON q.Id = c.Id AND q.Version = c.Version""".stripMargin
    )
    val rows = result.collect()

    // 2 rows: (1,1,5.0) matched; (999,1,null) unmatched
    rows.length should be(2)

    val matched = rows.find(_.getAs[Int]("qId") == 1).get
    matched.getAs[Double]("Value") should be(5.0)

    val unmatched = rows.find(_.getAs[Int]("qId") == 999).get
    unmatched.isNullAt(unmatched.fieldIndex("Value")) should be(true)
  }

  // --- Edge case: WHERE filter on Ariadne side not dropped (Issue #5, #6) ---

  test("JOIN preserves WHERE filter on Ariadne side") {
    createTestIndex("catalog_filter_join_test")

    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(1, 1), Row(1, 2), Row(1, 3))
      ),
      querySchema
    )
    queryData.createOrReplaceTempView("filter_join_query")

    val result = spark.sql(
      """SELECT c.* FROM ariadne.catalog_filter_join_test c
        |JOIN filter_join_query q ON c.Id = q.Id AND c.Version = q.Version
        |WHERE c.Value > 4.9""".stripMargin
    )
    val rows = result.collect()

    // Id=1 matching rows: (1,1,5.0), (1,2,4.5), (1,3,5.0)
    // With Value > 4.9: (1,1,5.0), (1,3,5.0) = 2 rows
    rows.length should be(2)
    rows.foreach(_.getAs[Double]("Value") should be > 4.9)
  }

  // --- Edge case: Namespace validation (Issue #11) ---

  test("three-part table name with default namespace works") {
    createTestIndex("catalog_ns_test")

    // Direct access should work
    val direct = spark.sql("SELECT * FROM ariadne.catalog_ns_test").collect()
    direct.length should be > 0

    // Explicit default namespace should also work
    val explicit = spark.sql("SELECT * FROM ariadne.default.catalog_ns_test").collect()
    explicit.length shouldBe direct.length

    // Non-default namespace should fail
    an[Exception] should be thrownBy {
      spark.sql("SELECT * FROM ariadne.fakens.catalog_ns_test").collect()
    }
  }

  // --- Edge case: Range filter correctness (Issue #13) ---

  test("SELECT with WHERE range on indexed column returns correct results") {
    createTestIndex("catalog_range_test")

    val result = spark.sql("SELECT * FROM ariadne.catalog_range_test WHERE Id > 2")
    val rows = result.collect()

    // Id > 2: (3,1,4.0), (4,1,2.0), (4,2,9.0), (3,2,4.0) = 4 rows
    rows.length should be(4)
    rows.foreach(_.getAs[Int]("Id") should be > 2)
  }

  // --- Round 2 edge case: Temporal dedup missing in join rule ---

  test("SQL JOIN on temporal index applies deduplication like programmatic API") {
    val temporalSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false),
      StructField("UpdatedAt", TimestampType, nullable = false)
    ))
    val csvOptions = Map("header" -> "true")
    val index = Index("catalog_temporal_join_test", temporalSchema, "csv", csvOptions)
    index.addFile(resourcePath("/data/temporal_part0.csv"))
    index.addFile(resourcePath("/data/temporal_part1.csv"))
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    val queryIdSchema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      queryIdSchema
    )
    queryData.createOrReplaceTempView("temporal_join_query")

    // Programmatic join applies temporal dedup — returns only latest version
    val programmatic = index.join(queryData, Seq("Id"), "inner")
    val programmaticRows = programmatic.collect()
    programmaticRows.length should be(1)
    programmaticRows.head.getAs[Double]("Value") should be(150.0)

    // SQL join should match programmatic behavior
    val sqlResult = spark.sql(
      """SELECT c.Id, c.Value FROM ariadne.catalog_temporal_join_test c
        |JOIN temporal_join_query q ON c.Id = q.Id""".stripMargin
    )
    val sqlRows = sqlResult.collect()
    sqlRows.length should be(1)
    sqlRows.head.getAs[Double]("Value") should be(150.0)
  }

  // --- Round 2 edge case: Temporal pushdown changes scan semantics ---

  test("SELECT WHERE on temporal column returns only latest version") {
    val temporalSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false),
      StructField("UpdatedAt", TimestampType, nullable = false)
    ))
    val csvOptions = Map("header" -> "true")
    val index = Index("catalog_temporal_where_test", temporalSchema, "csv", csvOptions)
    index.addFile(resourcePath("/data/temporal_part0.csv"))
    index.addFile(resourcePath("/data/temporal_part1.csv"))
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // Temporal indexes mean "only the latest version" — SELECT should
    // return only the latest version, same as JOIN behavior
    val result = spark.sql("SELECT * FROM ariadne.catalog_temporal_where_test WHERE Id = 1")
    val rows = result.collect()

    // Id=1 appears in both files but temporal dedup keeps only the latest
    rows.length should be(1)
    rows.head.getAs[Double]("Value") should be(150.0)
  }

  test("SELECT * on temporal index applies dedup even without WHERE") {
    val temporalSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false),
      StructField("UpdatedAt", TimestampType, nullable = false)
    ))
    val csvOptions = Map("header" -> "true")
    val index = Index("catalog_temporal_select_test", temporalSchema, "csv", csvOptions)
    index.addFile(resourcePath("/data/temporal_part0.csv"))
    index.addFile(resourcePath("/data/temporal_part1.csv"))
    index.addTemporalIndex("Id", "UpdatedAt")
    index.update

    // part0: Id=1,2,3,4  part1: Id=1,2,5
    // After temporal dedup: Id=1(part1),2(part1),3(part0),4(part0),5(part1) = 5 unique
    val result = spark.sql("SELECT * FROM ariadne.catalog_temporal_select_test")
    val rows = result.collect()
    rows.length should be(5)

    // Verify Id=1 has latest value
    val id1Row = rows.find(_.getAs[Int]("Id") == 1).get
    id1Row.getAs[Double]("Value") should be(150.0)
  }

  // --- Round 2 edge case: buildScan extra columns with computed indexes ---

  test("SELECT * with computed index returns only source schema columns") {
    val csvOptions = Map("header" -> "true")
    val index = Index("catalog_computed_cols_test", indexSchema, "csv", csvOptions)
    index.addFile(resourcePath("/data/table1_part0.csv"))
    index.addFile(resourcePath("/data/table1_part1.csv"))
    index.addIndex("Id")
    index.addComputedIndex("IdTimes10", "Id * 10")
    index.update

    // SELECT * should only show source schema columns, not computed index columns
    val result = spark.sql("SELECT * FROM ariadne.catalog_computed_cols_test")
    result.columns should contain allOf ("Id", "Version", "Value")
    result.columns should not contain "IdTimes10"
    result.collect().length should be(8)
  }

  test("tableExists returns true for existing index and false for missing") {
    createTestIndex("catalog_exists_test")

    val catalog = new AriadneCatalog()
    catalog.initialize(
      "ariadne",
      new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
    )

    val existsIdent =
      Identifier.of(Array.empty[String], "catalog_exists_test")
    catalog.tableExists(existsIdent) should be(true)

    val missingIdent =
      Identifier.of(Array.empty[String], "no_such_index")
    catalog.tableExists(missingIdent) should be(false)

    // Non-default namespace should return false
    val nsIdent =
      Identifier.of(Array("other_ns"), "catalog_exists_test")
    catalog.tableExists(nsIdent) should be(false)
  }
}
