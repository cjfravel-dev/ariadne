package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, StandardCopyOption}
import dev.cjfravel.ariadne.Index.DataFrameOps

class IndexTests extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: SparkContext = _
  private var tempDir: Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("ariadne-test-output-")
    sc = new SparkContext("local[*]", "TestAriadne")

    spark = SparkSession
      .builder()
      .config(sc.getConf)
      .config("spark.ariadne.storagePath", tempDir.toString)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    Context.spark = spark
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null) {
      deleteDirectory(tempDir)
    }
  }

  def deleteDirectory(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      Files
        .walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  val table1Schema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val table2Schema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false)
    )
  )

  test("storagePath") {
    assert(Context.storagePath.toString === tempDir.toString)
  }

  test("Requires schema") {
    assertThrows[SchemaNotProvidedException] {
      val index = Index("noschema")
    }
  }

  test("Requires same schema") {
    val index = Index("schema", table1Schema, "csv")
    val index2 = Index("schema", table1Schema, "csv")
    assertThrows[SchemaMismatchException] {
      val index3 = Index("schema", table2Schema, "csv")
    }
  }

  test("Can specify new schema") {
    val index = Index("schemachange", table1Schema, "csv")
    index.addIndex("Id")
    val index2 = Index("schemachange", table2Schema, "csv", true)
    assertThrows[IndexNotFoundInNewSchemaException] {
      val index3 = Index("schemachange2", table1Schema, "csv")
      index3.addIndex("Value")
      val index4 = Index("schemachange2", table2Schema, "csv", true)
    }
  }

  test("Requires same format") {
    val index = Index("format", table1Schema, "csv")
    val index2 = Index("format", table1Schema, "csv")
    assertThrows[FormatMismatchException] {
      val index3 = Index("format", table1Schema, "parquet")
    }
  }

  test("Only requires schema first time") {
    assertThrows[SchemaNotProvidedException] {
      val index = Index("noschema")
    }
    val index = Index("schema", table1Schema, "csv")
    val index2 = Index("schema")
  }

  test("Can retrieve same schema") {
    val index = Index("sameschema", table1Schema, "csv")
    assert(index.storedSchema === table1Schema)
  }

  test("Index addFile") {
    val index = Index("test", table1Schema, "csv")
    val path = getClass.getResource("/data/table1_part0.csv").getPath
    assert(index.isFileAdded(path) === false)
    index.addFile(path)
    assert(index.isFileAdded(path) === true)
  }

  test("Index addFiles") {
    val index = Index("test2", table1Schema, "csv")
    val paths = Array(
      getClass.getResource("/data/table1_part0.csv").getPath,
      getClass.getResource("/data/table1_part1.csv").getPath
    )
    paths.foreach(path => assert(index.isFileAdded(path) === false))
    index.addFile(paths: _*)
    paths.foreach(path => assert(index.isFileAdded(path) === true))

    // manual logger test
    index.addFile(paths: _*)
  }

  test("Read table1_part0") {
    val df = spark.read
      .option("header", "true")
      .schema(table1Schema)
      .csv(getClass.getResource("/data/table1_part0.csv").getPath)

    assert(df.count() == 4)
  }

  test("Read table1_part1") {
    val df = spark.read
      .option("header", "true")
      .schema(table1Schema)
      .csv(getClass.getResource("/data/table1_part1.csv").getPath)

    assert(df.count() == 4)
  }

  test("Read table1") {
    val df = spark.read
      .option("header", "true")
      .schema(table1Schema)
      .csv(
        getClass.getResource("/data/table1_part0.csv").getPath,
        getClass.getResource("/data/table1_part1.csv").getPath
      )

    assert(df.count() == 8)
  }

  test("Update index") {
    val index = Index("test2", table1Schema, "csv")
    val paths = Array(
      getClass.getResource("/data/table1_part0.csv").getPath,
      getClass.getResource("/data/table1_part1.csv").getPath
    )
    index.addFile(paths: _*)
    assert(index.unindexedFiles.size === 2)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update
    assert(index.unindexedFiles.size === 0)
    index.printIndex
    index.printMetadata
    assert(
      index
        .locateFiles(Map("Version" -> Array("3"), "Id" -> Array("1")))
        .size === 2
    )
    assert(index.locateFiles(Map("Value" -> Array(4.5))).size === 1)
    assert(index.locateFiles(Map("Value" -> Array(-1))).size === 0)
  }

  private def normalizeSchema(schema: StructType): StructType = StructType(
    schema.fields
      // spark will covert non-null to null when reading csv
      .map(f => StructField(f.name.toLowerCase, f.dataType, nullable = false))
      .sortBy(_.name)
  )

  test("Join") {
    val index = Index("table1", table1Schema, "csv")
    val paths = Array(
      getClass.getResource("/data/table1_part0.csv").getPath,
      getClass.getResource("/data/table1_part1.csv").getPath
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    val table2 = spark.read
      .schema(table2Schema)
      .option("header", "true")
      .csv(getClass.getResource("/data/table2_part0.csv").getPath)

    assert(table2.join(index, Seq("Version", "Id"), "left_semi").count === 1)
    assert(table2.join(index, Seq("Version"), "fullouter").count === 4)
    assert(table2.join(index, Seq("Version", "Id"), "fullouter").count === 8)
    assert(
      normalizeSchema(
        table2.join(index, Seq("Version"), "left_semi").schema
      ) === normalizeSchema(table2Schema)
    )

    assert(index.join(table2, Seq("Version", "Id"), "left_semi").count === 1)
    assert(index.join(table2, Seq("Version"), "fullouter").count === 4)
    assert(index.join(table2, Seq("Version", "Id"), "fullouter").count === 8)
    assert(
      normalizeSchema(
        index.join(table2, Seq("Version"), "left_semi").schema
      ) === normalizeSchema(table1Schema)
    )
  }

  test("index exists") {
    val index = Index("exists", table1Schema, "csv")
    assert(Index.exists(spark, "exists") === true)
    assert(Index.exists(spark, "doesntexist") === false)
  }

  test("remove index") {
    val index = Index("toremove", table1Schema, "csv")
    assert(Index.exists(spark, "toremove") === true)
    Index.remove(spark, "toremove")
    assert(Index.exists(spark, "toremove") === false)
  }
}
