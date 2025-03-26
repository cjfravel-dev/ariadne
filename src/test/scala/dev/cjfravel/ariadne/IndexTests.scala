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
    sc.setLogLevel("ERROR")

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

  test("Requires schema") {
    assertThrows[SchemaNotProvidedException] {
      val index = Index(spark, "noschema")
    }
  }

  test("Requires same schema") {
    val index = Index(spark, "schema", table1Schema, "csv")
    val index2 = Index(spark, "schema", table1Schema, "csv")
    assertThrows[IllegalArgumentException] {
      val index3 = Index(spark, "schema", table2Schema, "csv")
    }
  }

  test("Only requires schema first time") {
    assertThrows[SchemaNotProvidedException] {
      val index = Index(spark, "noschema")
    }
    val index = Index(spark, "schema", table1Schema, "csv")
    val index2 = Index(spark, "schema")
  }

  test("Can retrieve same schema") {
    val index = Index(spark, "sameschema", table1Schema, "csv")
    assert(index.storedSchema === table1Schema)
  }

  test("storagePath") {
    assert(Index.storagePath(spark) === tempDir.toString)
  }

  test("Index addFile") {
    val index = Index(spark, "test", table1Schema, "csv")
    val path = getClass.getResource("/data/table1_part0.csv").getPath
    assert(index.isFileAdded(path) === false)
    index.addFile(path)
    assert(index.isFileAdded(path) === true)
  }

  test("Index addFiles") {
    val index = Index(spark, "test2", table1Schema, "csv")
    val paths = Array(
      getClass.getResource("/data/table1_part0.csv").getPath,
      getClass.getResource("/data/table1_part1.csv").getPath
    )
    paths.foreach(path => assert(index.isFileAdded(path) === false))
    index.addFile(paths: _*)
    paths.foreach(path => assert(index.isFileAdded(path) === true))
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
    val index = Index(spark, "test2", table1Schema, "csv")
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

  test("Join") {
    val index = Index(spark, "table1", table1Schema, "csv")
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
    
    assert(index.join(table2, Seq("Version", "Id"), "left_semi").count === 1)
    assert(index.join(table2, Seq("Version"), "fullouter").count === 4)
    assert(index.join(table2, Seq("Version", "Id"), "fullouter").count === 8)
  }
}
