package dev.cjfravel.ariadne

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}

import scala.collection.JavaConverters._

import com.google.gson.{Gson, JsonObject, JsonParser}
import dev.cjfravel.ariadne.exceptions.{AriadneException, IndexLockException, StorageMigrationException}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests explicit storage-format versioning and alpha37+ physical migration preflight.
 */
class StorageMigrationTests extends SparkTests with Matchers {

  private val gson = new Gson()

  private def metadataPath(indexName: String): Path =
    new Path(new Path(IndexPathUtils.storagePath, indexName), "metadata.json")

  private def readMetadataJson(indexName: String): JsonObject = {
    val path = metadataPath(indexName)
    val input = path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)
    try {
      JsonParser.parseString(new String(input.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject
    } finally {
      input.close()
    }
  }

  private def readMetadataString(indexName: String): String = {
    val path = metadataPath(indexName)
    val input = path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)
    try {
      new String(input.readAllBytes(), StandardCharsets.UTF_8)
    } finally {
      input.close()
    }
  }

  private def readLock(path: Path): LockInfo = {
    val input = path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)
    try {
      gson.fromJson(new String(input.readAllBytes(), StandardCharsets.UTF_8), classOf[LockInfo])
    } finally {
      input.close()
    }
  }

  private def writeMetadataJson(indexName: String, json: JsonObject): Unit = {
    val path = metadataPath(indexName)
    val output = path.getFileSystem(spark.sparkContext.hadoopConfiguration).create(path, true)
    try {
      output.write(gson.toJson(json).getBytes(StandardCharsets.UTF_8))
    } finally {
      output.close()
    }
  }

  private def makeUnversioned(indexName: String): Unit = {
    val json = readMetadataJson(indexName)
    json.remove("metadata_version")
    json.remove("storage_format_version")
    writeMetadataJson(indexName, json)
  }

  private def copyFixtureTree(resource: String, destination: java.nio.file.Path): Unit = {
    val source = Paths.get(getClass.getResource(resource).toURI)
    val paths = Files.walk(source)
    try {
      paths.iterator().asScala.foreach { path =>
        val target = destination.resolve(source.relativize(path).toString)
        if (Files.isDirectory(path)) Files.createDirectories(target)
        else Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING)
      }
    } finally {
      paths.close()
    }
  }

  test("new indexes declare current metadata and storage format versions") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))

    Index("versioned_new_index", schema, "parquet")

    val json = readMetadataJson("versioned_new_index")
    json.get("metadata_version").getAsInt shouldBe 10
    json.get("storage_format_version").getAsInt shouldBe 3
  }

  test("real alpha37 fixture migrates and remains idempotent") {
    val indexName = "alpha37_fixture"
    copyFixtureTree("/fixtures/alpha37/index-root", tempDir.resolve(s"indexes/$indexName"))
    copyFixtureTree(
      "/fixtures/alpha37/filelist",
      tempDir.resolve(s"filelists/${IndexPathUtils.fileListName(indexName)}"))
    val fixedSourcePath = Paths.get("/tmp/ariadne-alpha37-source.json")
    Files.copy(
      Paths.get(getClass.getResource("/fixtures/alpha37/source.json").toURI),
      fixedSourcePath,
      StandardCopyOption.REPLACE_EXISTING)

    try {
      val beforeMetadata = readMetadataJson(indexName)
      beforeMetadata.has("metadata_version") shouldBe false
      beforeMetadata.has("storage_format_version") shouldBe false
      val indexPath = new Path(new Path(IndexPathUtils.storagePath, indexName), "index")
      val before = spark.read.format("delta").load(indexPath.toString)
      before.columns should contain("users")
      before.columns should not contain "user_id"
      before.columns should not contain "file_size"

      Index(indexName).locateFiles(Map("user_id" -> Array[Any](100))) should contain(
        "file:///tmp/ariadne-alpha37-source.json")

      val migrated = spark.read.format("delta").load(indexPath.toString)
      migrated.columns should contain("user_id")
      migrated.columns should contain("file_size")
      migrated.columns should not contain "users"
      migrated.where(col("file_size").isNull).count() shouldBe 0L
      val migratedMetadata = readMetadataJson(indexName)
      migratedMetadata.get("metadata_version").getAsInt shouldBe 10
      migratedMetadata.get("storage_format_version").getAsInt shouldBe 3

      val metadataAfterMigration = readMetadataString(indexName)
      val historyAfterMigration = DeltaTable.forPath(spark, indexPath.toString).history().count()
      Index(indexName).locateFiles(Map("user_id" -> Array[Any](100))) should not be empty
      readMetadataString(indexName) shouldBe metadataAfterMigration
      DeltaTable.forPath(spark, indexPath.toString).history().count() shouldBe historyAfterMigration
    } finally {
      Files.deleteIfExists(fixedSourcePath)
    }
  }

  test("indexes without exploded mappings skip exploded storage inspection") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "no_exploded_fast_path"
    val index = Index(indexName, schema, "parquet")
    val invalidIndexPath = new Path(index.storagePath, "index")
    val filesystem = invalidIndexPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    filesystem.mkdirs(invalidIndexPath)

    index.locateFiles(Map("Id" -> Array[Any](1))) shouldBe empty
  }

  test("plain open leaves unversioned metadata unchanged") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "unversioned_read_only_open"
    Index(indexName, schema, "parquet")
    makeUnversioned(indexName)
    val before = readMetadataString(indexName)

    Index(indexName)

    readMetadataString(indexName) shouldBe before
  }

  test("query migrates an unversioned alpha37 exploded index to current storage") {
    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val source =
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("e1", Array(Row(100))), Row("e2", Array(Row(101))))),
        schema)
    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/storage_migration_${System.currentTimeMillis()}"
    source.write.mode("overwrite").parquet(sourcePath)

    val index = Index("alpha37_query_migration", schema, "parquet")
    index.addFile(sourcePath)
    index.addExplodedFieldIndex("users", "id", "user_id")
    val oldStyleIndex =
      spark.read
        .parquet(sourcePath)
        .withColumn("filename", input_file_name())
        .select("filename", "users")
        .withColumn("temp", explode(col("users.id")))
        .groupBy("filename")
        .agg(collect_set(col("temp")).alias("users"))
    val indexRoot = new Path(IndexPathUtils.storagePath, "alpha37_query_migration")
    val mainPath = new Path(indexRoot, "index")
    val stagingPath = new Path(indexRoot, "staging")
    oldStyleIndex.write.format("delta").mode("overwrite").save(mainPath.toString)
    oldStyleIndex.write.format("delta").mode("overwrite").save(stagingPath.toString)
    makeUnversioned("alpha37_query_migration")

    val located = Index("alpha37_query_migration").locateFiles(Map("user_id" -> Array[Any](100)))

    located should not be empty
    Seq(mainPath, stagingPath).foreach { path =>
      val migrated = spark.read.format("delta").load(path.toString)
      migrated.columns should contain("user_id")
      migrated.columns should not contain "users"
      migrated.columns should contain("file_size")
      migrated.where(col("file_size").isNull).count() shouldBe 0L
    }
    readMetadataJson("alpha37_query_migration").get("storage_format_version").getAsInt shouldBe 3

    val metadataAfterFirstMigration = readMetadataString("alpha37_query_migration")
    val mainHistoryAfterFirstMigration = DeltaTable.forPath(spark, mainPath.toString).history().count()
    val stagingHistoryAfterFirstMigration = DeltaTable.forPath(spark, stagingPath.toString).history().count()

    Index("alpha37_query_migration").locateFiles(Map("user_id" -> Array[Any](100))) should not be empty

    readMetadataString("alpha37_query_migration") shouldBe metadataAfterFirstMigration
    DeltaTable.forPath(spark, mainPath.toString).history().count() shouldBe mainHistoryAfterFirstMigration
    DeltaTable.forPath(spark, stagingPath.toString).history().count() shouldBe stagingHistoryAfterFirstMigration
  }

  test("migration heartbeat prevents a long-running migration lock from becoming stale") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "migration_heartbeat"
    val index = Index(indexName, schema, "parquet")
    val lockPath = new Path(index.storagePath, ".update.lock")
    val lock = IndexLock(lockPath, indexName)
    val correlationId = "migration-owner"
    val originalTimeout = spark.conf.getOption("spark.ariadne.lockTimeout")
    val originalMaxWait = spark.conf.getOption("spark.ariadne.lockMaxWait")
    val originalRetry = spark.conf.getOption("spark.ariadne.lockRetryInterval")
    spark.conf.set("spark.ariadne.lockTimeout", "2")
    spark.conf.set("spark.ariadne.lockMaxWait", "1")
    spark.conf.set("spark.ariadne.lockRetryInterval", "1")

    lock.acquire(correlationId)
    val acquired = readLock(lockPath)
    try {
      index.withMigrationHeartbeat(lock, correlationId) { checkHeartbeat =>
        val refreshDeadline = System.nanoTime() + 2L * 1000L * 1000L * 1000L
        while (readLock(lockPath).lastRefreshedAt == acquired.lastRefreshedAt && System.nanoTime() < refreshDeadline) {
          Thread.sleep(10)
        }
        readLock(lockPath).lastRefreshedAt should not be acquired.lastRefreshedAt
        Thread.sleep(3500)
        intercept[IndexLockException] {
          IndexLock(lockPath, indexName).acquire("migration-contender")
        }
        checkHeartbeat()
      }
    } finally {
      lock.release(correlationId)
      Seq(
        "spark.ariadne.lockTimeout" -> originalTimeout,
        "spark.ariadne.lockMaxWait" -> originalMaxWait,
        "spark.ariadne.lockRetryInterval" -> originalRetry).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  test("migration heartbeat fails when lock ownership is replaced") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "migration_heartbeat_ownership"
    val index = Index(indexName, schema, "parquet")
    val lockPath = new Path(index.storagePath, ".update.lock")
    val lock = IndexLock(lockPath, indexName)
    val replacement = IndexLock(lockPath, indexName)
    val correlationId = "migration-owner"
    val replacementId = "replacement-owner"
    val originalTimeout = spark.conf.getOption("spark.ariadne.lockTimeout")
    spark.conf.set("spark.ariadne.lockTimeout", "2")

    lock.acquire(correlationId)
    val acquired = readLock(lockPath)
    try {
      intercept[StorageMigrationException] {
        index.withMigrationHeartbeat(lock, correlationId) { _ =>
          val refreshDeadline = System.nanoTime() + 2L * 1000L * 1000L * 1000L
          while (
            readLock(lockPath).lastRefreshedAt == acquired.lastRefreshedAt && System.nanoTime() < refreshDeadline
          ) {
            Thread.sleep(10)
          }
          readLock(lockPath).lastRefreshedAt should not be acquired.lastRefreshedAt
          lock.release(correlationId)
          replacement.acquire(replacementId)
          Thread.sleep(1200)
        }
      }
    } finally {
      replacement.release(replacementId)
      originalTimeout match {
        case Some(value) => spark.conf.set("spark.ariadne.lockTimeout", value)
        case None => spark.conf.unset("spark.ariadne.lockTimeout")
      }
    }
  }

  test("future storage versions fail before querying") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "future_storage_version"
    Index(indexName, schema, "parquet")
    val json = readMetadataJson(indexName)
    json.addProperty("storage_format_version", 999)
    writeMetadataJson(indexName, json)

    val error =
      intercept[AriadneException] {
        Index(indexName).locateFiles(Map("Id" -> Array[Any](1)))
      }
    error.getClass.getSimpleName shouldBe "UnsupportedStorageFormatVersionException"

    val catalogError =
      intercept[AriadneException] {
        IndexCatalog.describe(indexName)
      }
    catalogError.getClass.getSimpleName shouldBe "UnsupportedStorageFormatVersionException"
  }

  test("future metadata versions fail before querying") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "future_metadata_version"
    Index(indexName, schema, "parquet")
    val json = readMetadataJson(indexName)
    json.addProperty("metadata_version", 999)
    writeMetadataJson(indexName, json)

    val error =
      intercept[AriadneException] {
        Index(indexName).locateFiles(Map("Id" -> Array[Any](1)))
      }
    error.getClass.getSimpleName shouldBe "UnsupportedMetadataVersionException"
  }

  test("older explicit metadata versions are normalized on operational preflight") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "older_metadata_version"
    Index(indexName, schema, "parquet")
    val json = readMetadataJson(indexName)
    json.addProperty("metadata_version", 9)
    writeMetadataJson(indexName, json)

    Index(indexName).locateFiles(Map("Id" -> Array[Any](1)))

    readMetadataJson(indexName).get("metadata_version").getAsInt shouldBe 10
  }

  test("current tables may contain both a regular source array and its exploded alias") {
    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val source =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("e1", Array(Row(100))))), schema)
    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/storage_current_both_${System.currentTimeMillis()}"
    source.write.mode("overwrite").parquet(sourcePath)
    val indexName = "current_regular_and_exploded"
    val index = Index(indexName, schema, "parquet")
    index.addFile(sourcePath)
    index.addIndex("users")
    index.addExplodedFieldIndex("users", "id", "user_id")
    index.update
    makeUnversioned(indexName)

    Index(indexName).locateFiles(Map("user_id" -> Array[Any](100))) should not be empty
  }

  test("new exploded aliases on regular array indexes wait for update backfill") {
    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val source =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("e1", Array(Row(100))))), schema)
    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/storage_pending_alias_${System.currentTimeMillis()}"
    source.write.mode("overwrite").parquet(sourcePath)
    val indexName = "pending_exploded_alias"
    val index = Index(indexName, schema, "parquet")
    index.addFile(sourcePath)
    index.addIndex("users")
    index.update
    index.addExplodedFieldIndex("users", "id", "user_id")
    index.update

    index.locateFiles(Map("user_id" -> Array[Any](100))) should not be empty
  }

  test("current storage versions skip repeated exploded schema inspection") {
    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val source =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("e1", Array(Row(100))))), schema)
    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/storage_current_skip_${System.currentTimeMillis()}"
    source.write.mode("overwrite").parquet(sourcePath)
    val indexName = "current_skip_exploded_inspection"
    val index = Index(indexName, schema, "parquet")
    index.addFile(sourcePath)
    index.addExplodedFieldIndex("users", "id", "user_id")
    index.update
    val invalidStagingPath = new Path(index.storagePath, "staging")
    invalidStagingPath.getFileSystem(spark.sparkContext.hadoopConfiguration).mkdirs(invalidStagingPath)

    index.locateFiles(Map("user_id" -> Array[Any](100))) should not be empty
  }

  test("unversioned self-mapped exploded columns migrate without a false collision") {
    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val source =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("e1", Array(Row(100))))), schema)
    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/storage_self_mapped_${System.currentTimeMillis()}"
    source.write.mode("overwrite").parquet(sourcePath)
    val indexName = "self_mapped_exploded"
    val index = Index(indexName, schema, "parquet")
    index.addFile(sourcePath)
    index.addExplodedFieldIndex("users", "id", "users")
    index.update
    makeUnversioned(indexName)

    Index(indexName).locateFiles(Map("users" -> Array[Any](100))) should not be empty
    readMetadataJson(indexName).get("storage_format_version").getAsInt shouldBe 3
  }

  test("mutating lifecycle operations migrate without recursively acquiring the update lock") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "migration_lifecycle_locking"
    Index(indexName, schema, "parquet")

    val operations =
      Seq[() => Unit](
        () => Index(indexName).update,
        () => Index(indexName).compact(),
        () => Index(indexName).vacuum(),
        () => Index(indexName).deleteFiles("/missing-file.parquet"))

    operations.foreach { operation =>
      makeUnversioned(indexName)
      operation()
      readMetadataJson(indexName).get("storage_format_version").getAsInt shouldBe 3
      val lockPath = new Path(new Path(IndexPathUtils.storagePath, indexName), ".update.lock")
      lockPath.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(lockPath) shouldBe false
    }
  }

  test("failed physical migration leaves metadata unversioned and releases the lock") {
    val schema =
      StructType(
        Seq(
          StructField("event_id", StringType, nullable = false),
          StructField(
            "users",
            ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = false)))),
            nullable = false)))
    val source =
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("e1", Array(Row(100))))), schema)
    val sourcePath = s"${System.getProperty("java.io.tmpdir")}/storage_collision_${System.currentTimeMillis()}"
    source.write.mode("overwrite").parquet(sourcePath)
    val indexName = "storage_migration_collision"
    val index = Index(indexName, schema, "parquet")
    index.addFile(sourcePath)
    index.addExplodedFieldIndex("users", "id", "user_id")
    val conflicting =
      spark.read
        .parquet(sourcePath)
        .withColumn("filename", input_file_name())
        .select("filename", "users")
        .withColumn("temp", explode(col("users.id")))
        .groupBy("filename")
        .agg(collect_set(col("temp")).alias("users"))
        .withColumn("user_id", col("users"))
    val indexRoot = new Path(IndexPathUtils.storagePath, indexName)
    conflicting.write.format("delta").mode("overwrite").save(new Path(indexRoot, "index").toString)
    makeUnversioned(indexName)

    intercept[StorageMigrationException] {
      Index(indexName).locateFiles(Map("user_id" -> Array[Any](100)))
    }

    readMetadataJson(indexName).get("storage_format_version") shouldBe null
    val lockPath = new Path(indexRoot, ".update.lock")
    lockPath.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(lockPath) shouldBe false
  }

  test("missing source files prevent file-size migration and version advancement") {
    val schema = StructType(Seq(StructField("Id", IntegerType, nullable = false)))
    val indexName = "missing_file_size_source"
    Index(indexName, schema, "parquet")
    val sparkSession = spark
    import sparkSession.implicits._
    Seq(("/definitely/missing/source.parquet", Seq(1)))
      .toDF("filename", "Id")
      .write
      .format("delta")
      .mode("overwrite")
      .save(new Path(new Path(IndexPathUtils.storagePath, indexName), "index").toString)
    makeUnversioned(indexName)

    intercept[StorageMigrationException] {
      Index(indexName).locateFiles(Map("Id" -> Array[Any](1)))
    }

    readMetadataJson(indexName).get("storage_format_version") shouldBe null
  }
}
