package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source
import java.nio.charset.StandardCharsets
import com.google.gson.Gson
import java.nio.file.Paths
import java.nio.file.Files
import collection.JavaConverters._

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

  test("v1 -> v3") {
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
    assert(metadata.exploded_field_indexes.size() === 0)
  }

  test("v2 -> v3") {
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
    assert(metadata.exploded_field_indexes.size() === 0)
  }

  test("v3") {
    val stream = getClass.getResourceAsStream("/index_metadata/v3.json")
    require(stream != null, "Resource not found: /index_metadata/v3.json")
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
    assert(metadata.exploded_field_indexes.size() === 2)
    
    val explodedFieldIndexes = metadata.exploded_field_indexes.asScala.toSeq
    val userIdIndex = explodedFieldIndexes.find(_.as_column == "user_id").get
    assert(userIdIndex.array_column === "users")
    assert(userIdIndex.field_path === "id")
    assert(userIdIndex.as_column === "user_id")
    
    val tagNameIndex = explodedFieldIndexes.find(_.as_column == "tag_name").get
    assert(tagNameIndex.array_column === "tags")
    assert(tagNameIndex.field_path === "name")
    assert(tagNameIndex.as_column === "tag_name")
  }

  test("v3 -> v4") {
    val stream = getClass.getResourceAsStream("/index_metadata/v3.json")
    require(stream != null, "Resource not found: /index_metadata/v3.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.computed_indexes.size() === 2)
    assert(metadata.exploded_field_indexes.size() === 2)
    assert(metadata.read_options.size() === 0)
    assert(metadata.bloom_indexes.size() === 0)
  }

  test("v4") {
    val stream = getClass.getResourceAsStream("/index_metadata/v4.json")
    require(stream != null, "Resource not found: /index_metadata/v4.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.computed_indexes.size() === 2)
    assert(metadata.exploded_field_indexes.size() === 2)
    assert(metadata.read_options.size() === 2)
    assert(metadata.read_options.get("header") === "true")
    assert(metadata.read_options.get("multiLine") === "true")
    assert(metadata.bloom_indexes.size() === 0)
  }

  test("v4 -> v5") {
    val stream = getClass.getResourceAsStream("/index_metadata/v4.json")
    require(stream != null, "Resource not found: /index_metadata/v4.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.computed_indexes.size() === 2)
    assert(metadata.exploded_field_indexes.size() === 2)
    assert(metadata.read_options.size() === 2)
    // v4 -> v5 migration should add empty bloom_indexes
    assert(metadata.bloom_indexes.size() === 0)
  }

  test("v5") {
    val stream = getClass.getResourceAsStream("/index_metadata/v5.json")
    require(stream != null, "Resource not found: /index_metadata/v5.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.computed_indexes.size() === 2)
    assert(metadata.exploded_field_indexes.size() === 2)
    assert(metadata.read_options.size() === 2)
    assert(metadata.bloom_indexes.size() === 2)
    
    val bloomIndexes = metadata.bloom_indexes.asScala.toSeq
    val transactionIdIndex = bloomIndexes.find(_.column == "transaction_id").get
    assert(transactionIdIndex.column === "transaction_id")
    assert(transactionIdIndex.fpr === 0.01)
    
    val sessionIdIndex = bloomIndexes.find(_.column == "session_id").get
    assert(sessionIdIndex.column === "session_id")
    assert(sessionIdIndex.fpr === 0.001)
  }

  test("v1 -> v5 full migration") {
    val stream = getClass.getResourceAsStream("/index_metadata/v1.json")
    require(stream != null, "Resource not found: /index_metadata/v1.json")
    val jsonString = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    val metadata = IndexMetadata(jsonString)
    assert(metadata.format === "parquet")
    assert(metadata.schema === "not a real schema")
    assert(metadata.indexes.size() === 2)
    assert(metadata.computed_indexes.size() === 0)
    assert(metadata.exploded_field_indexes.size() === 0)
    assert(metadata.read_options.size() === 0)
    assert(metadata.bloom_indexes.size() === 0)
  }
}
