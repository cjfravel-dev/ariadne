package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source
import java.nio.charset.StandardCharsets
import com.google.gson.Gson
import java.nio.file.Paths
import java.nio.file.Files

class IndexMetadataTests extends AnyFunSuite {
  test("v1") {
    val stream = getClass.getResourceAsStream("/index_metadata/v1.json")
    require(stream != null, "Resource not found: /index_metadata/v1.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.indexes.contains("Test"))
    assert(metadata.indexes.contains("Test2"))
  }
  

  test("v1 -> v2") {
    val stream = getClass.getResourceAsStream("/index_metadata/v1.json")
    require(stream != null, "Resource not found: /index_metadata/v1.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.indexes.contains("Test"))
    assert(metadata.indexes.contains("Test2"))
    assert(metadata.computed_indexes.size() === 0)
  }

  test("v2") {
    val stream = getClass.getResourceAsStream("/index_metadata/v2.json")
    require(stream != null, "Resource not found: /index_metadata/v2.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.indexes.contains("Test"))
    assert(metadata.indexes.contains("Test2"))
    assert(metadata.computed_indexes.size() === 2)
    assert(metadata.computed_indexes.containsKey("test") === true)
    assert(metadata.computed_indexes.containsKey("test2") === true)
  }
}
