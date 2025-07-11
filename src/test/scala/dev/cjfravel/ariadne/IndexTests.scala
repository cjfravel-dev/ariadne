package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import dev.cjfravel.ariadne.Index.DataFrameOps
import java.nio.file.Files

class IndexTests extends SparkTests {
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

  val table3Schema = StructType(
    Seq(
      StructField("Id", StringType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val table4Schema = StructType(
    Seq(
      StructField("Id", StringType, nullable = false),
      StructField("Status", StringType, nullable = false)
    )
  )

  test("Requires schema") {
    assertThrows[SchemaNotProvidedException] {
      val index = Index("noschema")
    }
  }

  test("Requires same schema") {
    val csvOptions = Map("header" -> "true")
    val index = Index("schema", table1Schema, "csv", csvOptions)
    val index2 = Index("schema", table1Schema, "csv", csvOptions)
    assertThrows[SchemaMismatchException] {
      val index3 = Index("schema", table2Schema, "csv", csvOptions)
    }
  }

  test("Can specify new schema") {
    val csvOptions = Map("header" -> "true")
    val index = Index("schemachange", table1Schema, "csv", csvOptions)
    index.addIndex("Id")
    val index2 = Index("schemachange", table2Schema, "csv", true, csvOptions)
    assertThrows[IndexNotFoundInNewSchemaException] {
      val index3 = Index("schemachange2", table1Schema, "csv", csvOptions)
      index3.addIndex("Value")
      val index4 = Index("schemachange2", table2Schema, "csv", true, csvOptions)
    }
  }

  test("Requires same format") {
    val csvOptions = Map("header" -> "true")
    val index = Index("format", table1Schema, "csv", csvOptions)
    val index2 = Index("format", table1Schema, "csv", csvOptions)
    assertThrows[FormatMismatchException] {
      val index3 = Index("format", table1Schema, "parquet")
    }
  }

  test("Only requires schema first time") {
    val csvOptions = Map("header" -> "true")
    assertThrows[SchemaNotProvidedException] {
      val index = Index("noschema")
    }
    val index = Index("schema", table1Schema, "csv", csvOptions)
    val index2 = Index("schema")
  }

  test("Can retrieve same schema") {
    val csvOptions = Map("header" -> "true")
    val index = Index("sameschema", table1Schema, "csv", csvOptions)
    assert(index.storedSchema === table1Schema)
  }

  test("Index addFile") {
    val csvOptions = Map("header" -> "true")
    val index = Index("test", table1Schema, "csv", csvOptions)
    val path = resourcePath("/data/table1_part0.csv")
    assert(index.hasFile(path) === false)
    index.addFile(path)
    assert(index.hasFile(path) === true)
  }

  test("Index addFiles") {
    val csvOptions = Map("header" -> "true")
    val index = Index("test2", table1Schema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    paths.foreach(path => assert(index.hasFile(path) === false))
    index.addFile(paths: _*)
    paths.foreach(path => assert(index.hasFile(path) === true))

    // manual logger test
    index.addFile(paths: _*)
  }

  test("Read table1_part0") {
    val df = spark.read
      .option("header", "true")
      .schema(table1Schema)
      .csv(resourcePath("/data/table1_part0.csv"))

    assert(df.count() == 4)
  }

  test("Read table1_part1") {
    val df = spark.read
      .option("header", "true")
      .schema(table1Schema)
      .csv(resourcePath("/data/table1_part1.csv"))

    assert(df.count() == 4)
  }

  test("Read table1") {
    val df = spark.read
      .option("header", "true")
      .schema(table1Schema)
      .csv(
        resourcePath("/data/table1_part0.csv"),
        resourcePath("/data/table1_part1.csv")
      )

    assert(df.count() == 8)
  }

  test("Update index") {
    val csvOptions = Map("header" -> "true")
    val index = Index("update", table1Schema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    assert(index.unindexedFiles.size === 2)
    index.addIndex("Id")
    index.addIndex("Version")
    index.addIndex("Value")
    index.update
    assert(index.unindexedFiles.size === 0)
    index.printIndex(false)
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
    val csvOptions = Map("header" -> "true")
    val index = Index("join", table1Schema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    index.addFile(paths: _*)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update

    val table2 = spark.read
      .schema(table2Schema)
      .option("header", "true")
      .csv(resourcePath("/data/table2_part0.csv"))

    assert(table2.join(index, Seq("Version", "Id"), "left_semi").count === 1)
    assert(table2.join(index, Seq("Version"), "fullouter").count === 1)
    assert(table2.join(index, Seq("Version", "Id"), "fullouter").count === 1)
    assert(
      normalizeSchema(
        table2.join(index, Seq("Version"), "left_semi").schema
      ) === normalizeSchema(table2Schema)
    )

    assert(index.join(table2, Seq("Version", "Id"), "left_semi").count === 1)
    assert(index.join(table2, Seq("Version"), "fullouter").count === 1)
    assert(index.join(table2, Seq("Version", "Id"), "fullouter").count === 1)
    assert(
      normalizeSchema(
        index.join(table2, Seq("Version"), "left_semi").schema
      ) === normalizeSchema(table1Schema)
    )
  }

  test("index exists") {
    val csvOptions = Map("header" -> "true")
    val index = Index("exists", table1Schema, "csv", csvOptions)
    val path = resourcePath("/data/table1_part0.csv")
    index.addFile(path)
    assert(Index.exists("exists") === true)

    assert(Index.exists("doesntexist") === false)
  }

  test("remove index") {
    val csvOptions = Map("header" -> "true")
    val index = Index("toremove", table1Schema, "csv", csvOptions)
    val path = resourcePath("/data/table1_part0.csv")
    index.addFile(path)
    assert(Index.exists("toremove") === true)
    Index.remove("toremove")
    assert(Index.exists("toremove") === false)
  }

  test("computed_index") {
    val csvOptions = Map("header" -> "true")
    val index = Index("computed", table3Schema, "csv", csvOptions)
    val paths = Array(
      resourcePath("/data/table3_part0.csv"),
      resourcePath("/data/table3_part1.csv")
    )
    index.addFile(paths: _*)
    val column = "substring(Id, 1, 4)"
    index.addComputedIndex("Category", column)
    index.update

    assert(
      spark.read
        .schema(table4Schema)
        .option("header", "true")
        .csv(resourcePath("/data/table4_part0.csv"))
        .withColumn("Category", substring(col("Id"), 1, 4))
        .join(index, Seq("Category"), "inner")
        .count == 3
    )

    assert(
      spark.read
        .schema(table4Schema)
        .option("header", "true")
        .csv(resourcePath("/data/table4_part1.csv"))
        .withColumn("Category", substring(col("Id"), 1, 4))
        .join(index, Seq("Category"), "inner")
        .count == 2
    )

    assert(
      spark.read
        .schema(table4Schema)
        .option("header", "true")
        .csv(resourcePath("/data/table4_part0.csv"))
        .withColumn("Category", substring(col("Id"), 1, 4))
        .join(index, Seq("Category", "Id"), "inner")
        .count == 1
    )

    assert(
      spark.read
        .schema(table4Schema)
        .option("header", "true")
        .csv(resourcePath("/data/table4_part1.csv"))
        .withColumn("Category", substring(col("Id"), 1, 4))
        .join(index, Seq("Category", "Id"), "inner")
        .count == 1
    )
  }

  test("large index") {
    val df = spark.range(0, 1500000).toDF("Id")
      .withColumn("Guid", expr("uuid()").cast(StringType))
        
    val path = tempDir.resolve("large_index")
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .mode("overwrite")
      .csv(path.toString)

    val fileName = "file://" + Files
      .walk(path)
      .filter(Files.isRegularFile(_))
      .filter(_.getFileName.toString.endsWith(".csv"))
      .findFirst()
      .get()
      .toString

    val csvOptions = Map("header" -> "true")
    val index = Index("large_index", df.schema, "csv", csvOptions)
    index.addFile(fileName)
    index.addIndex("Id")
    index.addIndex("Guid")
    index.update
    assert(index.unindexedFiles.size === 0)

    val joinTestId = df
      .select("Id")
      .sample(0.2)
      .limit(100)
    assert(
      index
        .join(joinTestId, Seq("Id"), "left_semi")
        .count() === 100
    )

    val joinTestGuid = df
      .select("Guid")
      .sample(0.2)
      .limit(100)
    assert(
      index
        .join(joinTestGuid, Seq("Guid"), "left_semi")
        .count() === 100
    )

    val joinTestIdGuid = df
      .select("Id", "Guid")
      .sample(0.2)
      .limit(100)
    assert(
      index
        .join(joinTestIdGuid, Seq("Id", "Guid"), "left_semi")
        .count() === 100
    )
  }

  test("Array indexing") {
    // Define schema for array test data
    val arrayTestSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))), nullable = false),
      StructField("tags", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("category", StringType, nullable = false)
      ))), nullable = false)
    ))

    // Create test data using Row objects
    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("e1", "login", 
          Array(Row(100, "Alice"), Row(101, "Bob")), 
          Array(Row("security", "auth"), Row("monitoring", "system"))),
        Row("e2", "purchase", 
          Array(Row(101, "Bob"), Row(102, "Charlie")),
          Array(Row("commerce", "sales"), Row("payment", "billing"))),
        Row("e3", "logout", 
          Array(Row(100, "Alice")), 
          Array(Row("security", "auth")))
      )),
      arrayTestSchema
    )

    // Write test data to temporary location
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/array_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").parquet(tempPath)

    // Create index with array indexing
    val index = Index("array_test", arrayTestSchema, "parquet")
    index.addFile(tempPath)
    index.addIndex("event_type")  // regular index
    index.addExplodedFieldIndex("users", "id", "user_id")  // exploded field index: users[].id as "user_id"
    index.addExplodedFieldIndex("tags", "name", "tag_name")  // exploded field index: tags[].name as "tag_name"
    index.update

    // Test that we can find files by user_id
    val userFiles = index.locateFiles(Map("users" -> Array(100, 101)))
    assert(userFiles.nonEmpty, "Should find files containing user IDs 100 or 101")

    // Test that we can find files by tag
    val tagFiles = index.locateFiles(Map("tags" -> Array("security")))
    assert(tagFiles.nonEmpty, "Should find files containing security tag")

    // Test join with user_id
    val userQueryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(100, "login"),
        Row(101, "purchase")
      )),
      StructType(Seq(
        StructField("user_id", IntegerType, nullable = false),
        StructField("event_type", StringType, nullable = false)
      ))
    )
    val userJoinResult = userQueryData.join(index, Seq("user_id", "event_type"))
    assert(userJoinResult.count() > 0, "Should be able to join on user_id and event_type")

    // Test join with tag_name
    val tagQueryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("security"),
        Row("commerce")
      )),
      StructType(Seq(StructField("tag_name", StringType, nullable = false)))
    )
    val tagJoinResult = tagQueryData.join(index, Seq("tag_name"))
    assert(tagJoinResult.count() > 0, "Should be able to join on tag_name")

    // Test that array-derived columns appear in indexes
    assert(index.indexes.contains("user_id"), "user_id should be in indexes")
    assert(index.indexes.contains("tag_name"), "tag_name should be in indexes")
    assert(index.indexes.contains("event_type"), "event_type should be in indexes")

    // Clean up
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
  }

  // JSON Support Test Cases
  test("JSON format support - basic indexing") {
    // Define schema that matches the JSON array structure
    val jsonEventsSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("participants", ArrayType(StructType(Seq(
        StructField("role", StringType, nullable = true),
        StructField("user_id", LongType, nullable = true)
      ))), nullable = true),
      StructField("tags", ArrayType(StructType(Seq(
        StructField("category", StringType, nullable = true),
        StructField("name", StringType, nullable = true)
      ))), nullable = true)
    ))

    // Create index with JSON format and multiLine option for JSON arrays
    val readOptions = Map("multiLine" -> "true")
    val index = Index("json_events", jsonEventsSchema, "json", readOptions)
    val jsonPath = resourcePath("/data/array_events.json")
    
    // Test file operations
    assert(index.hasFile(jsonPath) === false)
    index.addFile(jsonPath)
    assert(index.hasFile(jsonPath) === true)
    
    // Test basic indexing
    index.addIndex("event_id")
    index.update
    assert(index.unindexedFiles.size === 0)
    
    // Print index for debugging
    index.printIndex(false)
    
    // Test file location by event_id
    val eventFiles = index.locateFiles(Map("event_id" -> Array("evt1", "evt2")))
    assert(eventFiles.nonEmpty, "Should find files containing event IDs evt1 or evt2")
    
    // Verify index stats
    val stats = index.stats()
    assert(stats.count() > 0, "Should have index statistics")
  }

  test("JSON format support - exploded field indexing") {
    // Define schema for JSON test data (matching array_test.json structure)
    val jsonTestSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("name", StringType, nullable = true)
      ))), nullable = true),
      StructField("tags", ArrayType(StringType), nullable = true)
    ))

    // Create index with ONLY exploded field indexing (no regular indexes) and multiLine option
    val readOptions = Map("multiLine" -> "true")
    val index = Index("json_test_exploded", jsonTestSchema, "json", readOptions)
    val jsonPath = resourcePath("/data/array_test.json")
    index.addFile(jsonPath)
    
    // Add ONLY exploded field index to test this specific functionality
    index.addExplodedFieldIndex("users", "id", "user_id")
    index.update
    
    // Print index for debugging
    index.printIndex(false)
    
    // Verify only the exploded field index is registered
    assert(index.indexes.contains("user_id"), "user_id should be in indexes")
    assert(!index.indexes.contains("event_type"), "event_type should NOT be in indexes for this test")
    
    // Test locating files by exploded field values - use correct storage column name
    val userIdFiles = index.locateFiles(Map("users" -> Array(100L, 101L)))
    assert(userIdFiles.nonEmpty, "Should find files containing user IDs 100 or 101")
    
    // Test join with ONLY exploded field (no regular columns)
    val userQueryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(100L),
        Row(101L)
      )),
      StructType(Seq(
        StructField("user_id", LongType, nullable = false)
      ))
    )
    val joinResult = userQueryData.join(index, Seq("user_id"))
    assert(joinResult.count() > 0, "Should be able to join on user_id from JSON exploded field data")
  }

  test("JSON format support - computed indexes") {
    // Simple schema for JSON computed index test
    val simpleJsonSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true)
    ))

    val readOptions = Map("multiLine" -> "true")
    val index = Index("json_computed", simpleJsonSchema, "json", readOptions)
    val jsonPath = resourcePath("/data/array_test.json")
    index.addFile(jsonPath)
    
    // Add computed index
    index.addComputedIndex("event_prefix", "substring(event_id, 1, 1)")
    // Don't add regular index - only test computed index
    index.update
    
    // Print index for debugging
    index.printIndex(false)
    
    // Verify only computed index is available
    assert(index.indexes.contains("event_prefix"), "computed index should be available")
    assert(!index.indexes.contains("event_type"), "event_type should NOT be in indexes for this test")
    
    // Test join with ONLY computed index
    val prefixQueryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("e")
      )),
      StructType(Seq(
        StructField("event_prefix", StringType, nullable = false)
      ))
    )
    val computedJoinResult = prefixQueryData.join(index, Seq("event_prefix"))
    println(s"Computed join result count: ${computedJoinResult.count()}")
    assert(computedJoinResult.count() > 0, "Should be able to join using computed index from JSON data")
  }

  test("JSON format validation - requires same format") {
    val jsonSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true)
    ))
    
    val index = Index("json_format_test", jsonSchema, "json")
    val index2 = Index("json_format_test", jsonSchema, "json")
    
    // Should throw exception when trying to change format
    assertThrows[FormatMismatchException] {
      val index3 = Index("json_format_test", jsonSchema, "parquet")
    }
  }

  test("JSON format support - schema validation") {
    val jsonSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true)
    ))
    
    val index = Index("json_schema_test", jsonSchema, "json")
    assert(index.storedSchema === jsonSchema)
    assert(index.format === "json")
  }
}
