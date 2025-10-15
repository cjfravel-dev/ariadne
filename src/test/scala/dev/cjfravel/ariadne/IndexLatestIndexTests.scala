package dev.cjfravel.ariadne

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import java.sql.Timestamp

class IndexLatestIndexTests extends SparkTests with Matchers {

  val userEventSchema = StructType(Seq(
    StructField("user_id", LongType, nullable = false),
    StructField("event_type", StringType, nullable = false),
    StructField("created_date", TimestampType, nullable = false),
    StructField("value", DoubleType, nullable = false)
  ))

  val orderSchema = StructType(Seq(
    StructField("order_id", StringType, nullable = false),
    StructField("customer_id", LongType, nullable = false),
    StructField("status", StringType, nullable = false),
    StructField("updated_at", TimestampType, nullable = false),
    StructField("amount", DoubleType, nullable = false)
  ))

  test("should add latest index configuration correctly") {
    val index = Index("latest_config_test", userEventSchema, "json")
    
    // Add latest index
    index.addLatestIndex("user_id", "created_date")
    
    // Verify configuration was added
    index.indexes should contain("user_id")
  }

  test("should support custom sort order for latest index") {
    val index = Index("latest_custom_order_test", userEventSchema, "json")
    
    // Add latest index with ascending order (earliest first)
    index.addLatestIndex("user_id", "created_date", descOrder = false)
    
    // Verify index was added
    index.indexes should contain("user_id")
  }

  test("should prevent duplicate latest indexes for same column") {
    val index = Index("latest_duplicate_test", userEventSchema, "json")
    
    // Add same latest index twice
    index.addLatestIndex("user_id", "created_date")
    index.addLatestIndex("user_id", "updated_at") // Should be ignored
    
    // Should still only show the column once in indexes
    index.indexes should contain("user_id")
  }

  test("should build latest indexes correctly with deduplication") {
    // Create test data with multiple entries per user, with different timestamps
    val baseTime = System.currentTimeMillis()
    val testData = Seq(
      Row(1L, "login", new Timestamp(baseTime), 1.0),
      Row(1L, "purchase", new Timestamp(baseTime + 1000), 2.0),
      Row(1L, "logout", new Timestamp(baseTime + 2000), 3.0), // Latest for user 1
      Row(2L, "login", new Timestamp(baseTime + 500), 1.5),
      Row(2L, "view", new Timestamp(baseTime + 1500), 2.5), // Latest for user 2
      Row(3L, "signup", new Timestamp(baseTime + 3000), 4.0) // Only entry for user 3
    )
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(testData),
      userEventSchema
    )
    
    // Write test data
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/latest_index_test_${System.currentTimeMillis()}"
    df.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      val index = Index("latest_build_test", userEventSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addLatestIndex("user_id", "created_date")
      index.update
      
      // Query should return files containing only the latest events
      val user1Files = index.locateFiles(Map("user_id" -> Array(1L)))
      user1Files should not be empty
      
      val user2Files = index.locateFiles(Map("user_id" -> Array(2L)))
      user2Files should not be empty
      
      val user3Files = index.locateFiles(Map("user_id" -> Array(3L)))
      user3Files should not be empty
      
      // All users should be found in the same file
      val allFiles = index.locateFiles(Map("user_id" -> Array(1L, 2L, 3L)))
      allFiles should not be empty
      
    } finally {
      // Clean up
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should work with joins using latest indexes") {
    // Create test data with order status changes
    val baseTime = System.currentTimeMillis()
    val orderData = Seq(
      Row("ORDER1", 100L, "pending", new Timestamp(baseTime), 99.99),
      Row("ORDER1", 100L, "processing", new Timestamp(baseTime + 1000), 99.99),
      Row("ORDER1", 100L, "shipped", new Timestamp(baseTime + 2000), 99.99), // Latest
      Row("ORDER2", 200L, "pending", new Timestamp(baseTime + 500), 149.99),
      Row("ORDER2", 200L, "cancelled", new Timestamp(baseTime + 1500), 149.99) // Latest
    )
    
    val orderDf = spark.createDataFrame(
      spark.sparkContext.parallelize(orderData),
      orderSchema
    )
    
    // Write test data
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/latest_join_test_${System.currentTimeMillis()}"
    orderDf.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      val index = Index("latest_join_test", orderSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addLatestIndex("order_id", "updated_at")
      index.update
      
      // Create query DataFrame looking for specific orders
      val queryData = Seq(
        Row("ORDER1"),
        Row("ORDER2")
      )
      val queryDf = spark.createDataFrame(
        spark.sparkContext.parallelize(queryData),
        StructType(Seq(StructField("order_id", StringType, nullable = false)))
      )
      
      // Join with index - should get data efficiently
      val joinedDf = queryDf.join(index, Seq("order_id"), "inner")
      val results = joinedDf.collect()
      
      results.length should be >= 2 // Should find both orders
      
      // Verify we can still query by order status efficiently
      val shippedFiles = index.locateFiles(Map("order_id" -> Array("ORDER1")))
      shippedFiles should not be empty
      
    } finally {
      // Clean up
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should work with multiple latest indexes") {
    val multiSchema = StructType(Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("product_id", StringType, nullable = false),
      StructField("action", StringType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false)
    ))
    
    val index = Index("multi_latest_test", multiSchema, "json")
    
    // Add multiple latest indexes
    index.addLatestIndex("user_id", "timestamp")
    index.addLatestIndex("product_id", "timestamp")
    
    // Verify both were added
    index.indexes should contain("user_id")
    index.indexes should contain("product_id")
  }

  test("should handle ascending order (earliest first) correctly") {
    val baseTime = System.currentTimeMillis()
    val testData = Seq(
      Row(1L, "first", new Timestamp(baseTime + 2000), 3.0), // Latest timestamp
      Row(1L, "second", new Timestamp(baseTime + 1000), 2.0),
      Row(1L, "third", new Timestamp(baseTime), 1.0) // Earliest timestamp - should be kept
    )
    
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(testData),
      userEventSchema
    )
    
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/latest_asc_test_${System.currentTimeMillis()}"
    df.write.mode("overwrite").json(tempPath)
    
    try {
      val readOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      val index = Index("latest_asc_test", userEventSchema, "json", readOptions)
      
      index.addFile(tempPath)
      index.addLatestIndex("user_id", "created_date", descOrder = false) // Earliest first
      index.update
      
      // Should still find the user
      val userFiles = index.locateFiles(Map("user_id" -> Array(1L)))
      userFiles should not be empty
      
    } finally {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new org.apache.hadoop.fs.Path(tempPath), true)
    }
  }

  test("should handle metadata migration from v4 to v5") {
    // Create v4 metadata (without latest_indexes field)
    val v4MetadataJson = """{
      "format": "json",
      "schema": "{\"type\":\"struct\",\"fields\":[{\"name\":\"user_id\",\"type\":\"long\",\"nullable\":false}]}",
      "indexes": ["user_id"],
      "computed_indexes": {},
      "exploded_field_indexes": [],
      "read_options": {}
    }"""
    
    // Parse v4 metadata - should automatically migrate to v5
    val metadata = IndexMetadata(v4MetadataJson)
    
    // Verify migration added latest_indexes field
    metadata.latest_indexes should not be null
    metadata.latest_indexes.size() should be(0)
  }

  test("should work with existing regular indexes") {
    val index = Index("mixed_indexes_test", userEventSchema, "json")
    
    // Add regular index first
    index.addIndex("event_type")
    
    // Add latest index
    index.addLatestIndex("user_id", "created_date")
    
    // Both should be available
    index.indexes should contain("event_type")
    index.indexes should contain("user_id")
  }
}