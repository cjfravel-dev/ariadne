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
    val path = resourcePath("/data/table1_part0.csv")
    assert(index.hasFile(path) === false)
    index.addFile(path)
    assert(index.hasFile(path) === true)
  }

  test("Index addFiles") {
    val index = Index("test2", table1Schema, "csv")
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
    val index = Index("update", table1Schema, "csv")
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
    val index = Index("join", table1Schema, "csv")
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
    val index = Index("exists", table1Schema, "csv")
    val path = resourcePath("/data/table1_part0.csv")
    index.addFile(path)
    assert(Index.exists("exists") === true)

    assert(Index.exists("doesntexist") === false)
  }

  test("remove index") {
    val index = Index("toremove", table1Schema, "csv")
    val path = resourcePath("/data/table1_part0.csv")
    index.addFile(path)
    assert(Index.exists("toremove") === true)
    Index.remove("toremove")
    assert(Index.exists("toremove") === false)
  }

  test("computed_index") {
    val index = Index("computed", table3Schema, "csv")
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

    val index = Index("large_index", df.schema, "csv")
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
}
