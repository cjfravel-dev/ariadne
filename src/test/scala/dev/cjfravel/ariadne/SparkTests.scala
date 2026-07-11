package dev.cjfravel.ariadne

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Base test infrastructure trait for all Ariadne Spark tests.
 *
 * Creates a local-mode `SparkSession` with Delta Lake extensions and a temporary directory as
 * `spark.ariadne.storagePath`. The session is torn down after each suite.
 *
 * Provides `resourcePath(fileName)` to resolve test data files from `src/test/resources/`.
 */
trait SparkTests extends AnyFunSuite with BeforeAndAfterAll {
  implicit var spark: SparkSession = _
  var sc: SparkContext = _
  var tempDir: Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("ariadne-test-output-")
    // spark.sql.extensions must be set on the SparkConf before the SparkContext is created so the
    // Delta session extension is installed; Delta path-based commands (e.g. VACUUM) rely on it.
    val conf =
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("TestAriadne")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.default.parallelism", "4")
        .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)

    spark =
      SparkSession
        .builder()
        .config(sc.getConf)
        .config("spark.ariadne.storagePath", tempDir.toString)
        .config("spark.ariadne.largeIndexLimit", "500000")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null) {
      // deleteDirectory(tempDir)
    }
  }

  def resourcePath(fileName: String): String =
    "file://" + getClass.getResource(fileName).getPath
}
