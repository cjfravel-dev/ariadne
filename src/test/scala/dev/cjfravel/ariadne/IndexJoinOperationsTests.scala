package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import dev.cjfravel.ariadne.Index.DataFrameOps

class IndexJoinOperationsTests extends SparkTests with Matchers {

  val indexSchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false),
      StructField("Value", DoubleType, nullable = false)
    )
  )

  val querySchema = StructType(
    Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Version", IntegerType, nullable = false)
    )
  )

  val explodedSchema = StructType(Seq(
    StructField("event_id", StringType, nullable = true),
    StructField("users", ArrayType(StructType(Seq(
      StructField("id", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))), nullable = true)
  ))

  test("should perform basic inner join correctly") {
    val index = Index("join_basic_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.addIndex("Version")
    index.update
    
    // Create query DataFrame
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 1),
        Row(2, 2)
      )),
      querySchema
    )
    
    // Test DataFrame.join(index, ...)
    val result1 = queryData.join(index, Seq("Id", "Version"), "inner")
    result1.count() should be > 0L
    result1.columns should contain allOf("Id", "Version", "Value")
    
    // Test index.join(DataFrame, ...)
    val result2 = index.join(queryData, Seq("Id", "Version"), "inner")
    result2.count() should be > 0L
    result2.columns should contain allOf("Id", "Version", "Value")
  }

  test("should perform left semi join correctly") {
    val index = Index("join_semi_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 1),
        Row(2, 2),
        Row(999, 999) // This ID shouldn't exist in the index
      )),
      querySchema
    )
    
    val result = queryData.join(index, Seq("Id"), "left_semi")
    result.count() should be < queryData.count()
    result.columns should not contain("Value") // Semi join should only return left side columns
  }

  test("should perform full outer join correctly") {
    val index = Index("join_outer_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 1),
        Row(999, 999) // This ID shouldn't exist in the index
      )),
      querySchema
    )
    
    val result = index.join(queryData, Seq("Id"), "fullouter")
    result.count() should be >= queryData.count()
    result.columns should contain allOf("Id", "Version", "Value")
  }

  test("should handle joins with single column") {
    val index = Index("join_single_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Version")
    index.update
    
    val singleColumnSchema = StructType(Seq(StructField("Version", IntegerType, nullable = false)))
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1),
        Row(2)
      )),
      singleColumnSchema
    )
    
    val result = queryData.join(index, Seq("Version"), "inner")
    result.count() should be > 0L
    result.columns should contain allOf("Version", "Id", "Value")
  }

  test("should handle joins with exploded field indexes") {
    // Create test data with arrays
    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("evt1", Array(Row(100L, "Alice"), Row(101L, "Bob"))),
        Row("evt2", Array(Row(102L, "Charlie"))),
        Row("evt3", Array(Row(100L, "Alice"), Row(103L, "David")))
      )),
      explodedSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/join_exploded_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("join_exploded_test", explodedSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addExplodedFieldIndex("users", "id", "user_id")
      index.update
      
      // Create query data using the exploded field column name
      val userQuerySchema = StructType(Seq(StructField("user_id", LongType, nullable = false)))
      val queryData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(100L),
          Row(101L)
        )),
        userQuerySchema
      )
      
      val result = queryData.join(index, Seq("user_id"), "inner")
      result.count() should be > 0L
      result.columns should contain allOf("user_id", "event_id")
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle joins with computed indexes") {
    val index = Index("join_computed_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addComputedIndex("id_doubled", "Id * 2")
    index.update
    
    val computedSchema = StructType(Seq(StructField("id_doubled", IntegerType, nullable = false)))
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(2),  // 2 * 1
        Row(4),  // 2 * 2
        Row(6)   // 2 * 3
      )),
      computedSchema
    )
    
    val result = queryData.join(index, Seq("id_doubled"), "inner")
    result.count() should be > 0L
    result.columns should contain allOf("id_doubled", "Id", "Version", "Value")
  }

  test("should handle joins with mixed index types") {
    val mixedSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("priority", IntegerType, nullable = true),
      StructField("users", ArrayType(StructType(Seq(
        StructField("id", LongType, nullable = true),
        StructField("role", StringType, nullable = true)
      ))), nullable = true)
    ))
    
    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("evt1", 1, Array(Row(100L, "admin"))),
        Row("evt2", 2, Array(Row(101L, "user"))),
        Row("evt3", 3, Array(Row(102L, "admin")))
      )),
      mixedSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/join_mixed_test_${System.currentTimeMillis()}"
    testData.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("multiLine" -> "true")
      val index = Index("join_mixed_test", mixedSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addIndex("event_id") // Regular index
      index.addComputedIndex("priority_level", "case when priority > 2 then 'high' else 'low' end") // Computed
      index.addExplodedFieldIndex("users", "id", "user_id") // Exploded field
      index.update
      
      // Test join with all three index types
      val multiQuerySchema = StructType(Seq(
        StructField("event_id", StringType, nullable = false),
        StructField("priority_level", StringType, nullable = false),
        StructField("user_id", LongType, nullable = false)
      ))
      
      val queryData = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("evt1", "low", 100L),
          Row("evt3", "high", 102L)
        )),
        multiQuerySchema
      )
      
      val result = queryData.join(index, Seq("event_id", "priority_level", "user_id"), "inner")
      result.count() should be > 0L
      result.columns should contain allOf("event_id", "priority_level", "user_id", "priority")
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle joins with no matching data") {
    val index = Index("join_no_match_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(999, 999),
        Row(998, 998)
      )),
      querySchema
    )
    
    // Inner join should return no results
    val innerResult = queryData.join(index, Seq("Id"), "inner")
    innerResult.count() should be(0)
    
    // Left join should return query data with nulls
    val leftResult = queryData.join(index, Seq("Id"), "left")
    leftResult.count() should be(queryData.count())
  }

  test("should cache join results for performance") {
    val index = Index("join_cache_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    val queryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 1),
        Row(2, 2)
      )),
      querySchema
    )
    
    // First join - should populate cache
    val result1 = queryData.join(index, Seq("Id"), "inner")
    val count1 = result1.count()
    
    // Second join with same parameters - should use cache
    val result2 = queryData.join(index, Seq("Id"), "inner")
    val count2 = result2.count()
    
    count1 should equal(count2)
    count1 should be > 0L
  }

  test("should use broadcast join when threshold is exceeded") {
    // Set a very low broadcast join threshold to trigger broadcast joins
    spark.conf.set("spark.ariadne.broadcastJoinThreshold", "5")
    
    val index = Index("join_broadcast_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    // Create query data that exceeds the broadcast threshold (5)
    val largeQueryData = spark.createDataFrame(
      spark.sparkContext.parallelize((1 to 10).map(i => Row(i, i))),
      querySchema
    )
    
    // This should trigger broadcast join filtering due to having more than 5 distinct values
    val result = largeQueryData.join(index, Seq("Id"), "inner")
    result.count() should be > 0L
    result.columns should contain allOf("Id", "Version", "Value")
    
    // Reset the configuration
    spark.conf.unset("spark.ariadne.broadcastJoinThreshold")
  }

  test("should handle null values in broadcast join filtering") {
    // Set a very low broadcast join threshold to trigger broadcast joins
    spark.conf.set("spark.ariadne.broadcastJoinThreshold", "3")
    
    val index = Index("join_broadcast_null_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    // Create query data with null values that exceeds the broadcast threshold
    val nullableSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = true),
      StructField("Version", IntegerType, nullable = true)
    ))
    
    val queryDataWithNulls = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 1),
        Row(2, 2),
        Row(3, 3),
        Row(null, 4), // null value that should be filtered out
        Row(5, null), // null value that should be filtered out
        Row(6, 6)
      )),
      nullableSchema
    )
    
    // This should trigger broadcast join filtering and handle null values gracefully
    val result = queryDataWithNulls.join(index, Seq("Id"), "inner")
    result.count() should be > 0L
    result.columns should contain allOf("Id", "Version", "Value")
    
    // Verify that null values are properly excluded from results
    val resultIds = result.select("Id").collect().map(_.getInt(0)).toSet
    resultIds should not contain null
    
    // Reset the configuration
    spark.conf.unset("spark.ariadne.broadcastJoinThreshold")
  }

  test("should handle all null values in broadcast join filtering") {
    // Set a very low broadcast join threshold to trigger broadcast joins
    spark.conf.set("spark.ariadne.broadcastJoinThreshold", "2")
    
    val index = Index("join_broadcast_all_null_test", indexSchema, "csv", Map("header" -> "true"))
    
    val csvPath = resourcePath("/data/table1_part0.csv")
    index.addFile(csvPath)
    index.addIndex("Id")
    index.update
    
    // Create query data with only null values that exceeds the broadcast threshold
    val nullableSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = true),
      StructField("Version", IntegerType, nullable = true)
    ))
    
    val allNullQueryData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(null, 1),
        Row(null, 2),
        Row(null, 3)
      )),
      nullableSchema
    )
    
    // This should trigger broadcast join filtering and return empty result since all values are null
    val result = allNullQueryData.join(index, Seq("Id"), "inner")
    result.count() should be(0L)
    
    // Reset the configuration
    spark.conf.unset("spark.ariadne.broadcastJoinThreshold")
  }

  test("should handle mixed data types in broadcast join filtering") {
    // Set a very low broadcast join threshold to trigger broadcast joins
    spark.conf.set("spark.ariadne.broadcastJoinThreshold", "3")
    
    // Create an index with string IDs
    val stringIndexSchema = StructType(Seq(
      StructField("StringId", StringType, nullable = true),
      StructField("Value", DoubleType, nullable = false)
    ))
    
    val stringTestData = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("id1", 1.0),
        Row("id2", 2.0),
        Row("id3", 3.0),
        Row("id4", 4.0)
      )),
      stringIndexSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/string_index_test_${System.currentTimeMillis()}"
    stringTestData.write.mode("overwrite").json(tempPath)
    
    try {
      val index = Index("join_string_broadcast_test", stringIndexSchema, "json")
      index.addFile(tempPath)
      index.addIndex("StringId")
      index.update
      
      // Create query data with string values and nulls
      val stringQuerySchema = StructType(Seq(StructField("StringId", StringType, nullable = true)))
      val queryDataWithStringNulls = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("id1"),
          Row("id2"),
          Row("id3"),
          Row(null), // null value that should be filtered out
          Row("id4")
        )),
        stringQuerySchema
      )
      
      // This should trigger broadcast join filtering with string data types
      val result = queryDataWithStringNulls.join(index, Seq("StringId"), "inner")
      result.count() should be > 0L
      result.columns should contain allOf("StringId", "Value")
      
      // Verify that null values are properly excluded
      val resultIds = result.select("StringId").collect().map(_.getString(0)).toSet
      resultIds should not contain null
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
    
    // Reset the configuration
    spark.conf.unset("spark.ariadne.broadcastJoinThreshold")
  }
}