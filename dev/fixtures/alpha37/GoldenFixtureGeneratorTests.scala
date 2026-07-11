package dev.cjfravel.ariadne

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import org.apache.spark.sql.types._

class GoldenFixtureGeneratorTests extends SparkTests {

  private def copyTree(source: Path, destination: Path): Unit = {
    Files
      .walk(source)
      .forEach { path =>
        val target = destination.resolve(source.relativize(path).toString)
        if (Files.isDirectory(path)) Files.createDirectories(target)
        else Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING)
      }
  }

  test("generate alpha37 golden fixture") {
    val output = Paths.get(sys.env("GOLDEN_OUT"))
    val sourcePath = Paths.get("/tmp/ariadne-alpha37-source.json")
    val sourceJson =
      """{"event_id":"e1","users":[{"id":100},{"id":101}]}
        |{"event_id":"e2","users":[{"id":200}]}
        |""".stripMargin
    Files.write(sourcePath, sourceJson.getBytes(StandardCharsets.UTF_8))

    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val index = Index("alpha37_fixture", schema, "json")
    index.addFile(sourcePath.toUri.toString)
    index.addExplodedFieldIndex("users", "id", "user_id")
    index.update

    Files.createDirectories(output)
    copyTree(tempDir.resolve("indexes/alpha37_fixture"), output.resolve("index-root"))
    copyTree(tempDir.resolve("filelists/[ariadne_index] alpha37_fixture"), output.resolve("filelist"))
    Files.copy(sourcePath, output.resolve("source.json"), StandardCopyOption.REPLACE_EXISTING)
  }
}
