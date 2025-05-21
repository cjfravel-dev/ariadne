package dev.cjfravel.ariadne

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

trait SparkTests extends AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _
  var sc: SparkContext = _
  var tempDir: Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("ariadne-test-output-")
    sc = new SparkContext("local[*]", "TestAriadne")

    spark = SparkSession
      .builder()
      .config(sc.getConf)
      .config("spark.ariadne.storagePath", tempDir.toString)
      .config("spark.ariadne.overflowLimit", "500000")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    AriadneContext.setSparkSession(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null) {
      // deleteDirectory(tempDir)
    }
  }

  private def deleteDirectory(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      Files
        .walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }

  def resourcePath(fileName: String): String = {
    "file://" + getClass.getResource(fileName).getPath
  }
}