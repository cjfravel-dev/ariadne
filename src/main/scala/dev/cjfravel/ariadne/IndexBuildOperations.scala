package dev.cjfravel.ariadne

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import dev.cjfravel.ariadne.exceptions._
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, LongType, StructField}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SerializableConfiguration

/**
 * Trait providing index building and maintenance operations for [[Index]] instances.
 *
 * This trait implements the core data pipeline for building and maintaining file-level indexes. It is mixed into
 * [[Index]] via the trait hierarchy and handles the complete update lifecycle:
 *
 * Update pipeline (data flow):
 *   1. [[analyzeFiles]] — Pre-flight scan that computes per-file distinct value counts for all indexed columns. This
 *      information drives batching decisions.
 *   2. [[createOptimalBatches]] — Groups files into batches so that the sum of `maxDistinctCount` per batch stays at or
 *      below `largeIndexLimit`. Files that individually exceed the limit are isolated into single-file batches.
 *   3. Per-batch processing (in `updateSingleBatch`):
 *      - Read source files, apply computed indexes, add filename column
 *      - [[classifyLargeFiles]] — Decide, per `(file, column)` pair, whether the file contributes enough distinct
 *        values to be stored out of line, reusing the counts from step 1
 *      - Build regular indexes (array aggregation per file, skipping large pairs)
 *      - Build exploded field, bloom filter, temporal, and range indexes
 *      - Build auto-bloom filters for columns with at least one large file
 *      - [[appendToStaging]] (inline columns to staging Delta table)
 *      - [[appendToLargeIndex]] (large columns streamed to per-column Delta tables)
 *   4. Periodic consolidation — Every `stagingConsolidationThreshold` batches, [[consolidateStaging]] merges staging
 *      into the main index via Delta MERGE (upsert on filename). Auto-compaction may follow via [[maybeAutoCompact]].
 *   5. Final consolidation — After all batches complete, any remaining staged data is consolidated and the staging
 *      table is deleted.
 *
 * Batching strategy: Files are sorted by `maxDistinctCount` (largest first) and packed sequentially into batches. This
 * greedy approach keeps each batch at or below the large index limit while maximizing batch sizes for efficiency.
 *
 * Staging: Each batch is appended to a transient staging Delta table. Periodic consolidation merges staged rows into
 * the main index, providing fault tolerance (partially processed updates can be recovered).
 *
 * Large index handling: Columns whose array size exceeds `largeIndexLimit` for any file are stored in separate Delta
 * tables under `large_indexes/{column}/` as exploded (one row per value) rather than array-per-file. Auto-bloom filters
 * are built for these columns in the main index to enable pre-filtering at query time.
 *
 * @see
 *   [[BloomFilterOperations]] for bloom filter creation and querying
 * @see
 *   [[Index.update]] for the public entry point
 */
trait IndexBuildOperations extends BloomFilterOperations {
  self: Index =>

  private val StagingTimestampColumn = "_ariadne_staged_at"
  private val StagingBatchIdColumn = "_ariadne_batch_id"
  private val StagingRankColumn = "_ariadne_staging_rank"
  private val StagingCompletenessColumn = "_ariadne_staging_completeness"
  private val StagingHashColumn = "_ariadne_staging_hash"
  private val ReservedStagingColumnNames =
    Set(StagingTimestampColumn, StagingBatchIdColumn, StagingRankColumn, StagingCompletenessColumn, StagingHashColumn)

  protected def requireNonReservedStagingColumn(column: String): Unit =
    require(!ReservedStagingColumnNames.contains(column), s"Column '$column' is reserved for Ariadne staging metadata")

  /**
   * Rejects nested (dotted) paths for columns that become index column names.
   *
   * An indexed value column is persisted as a column of the index table under its own name, and is later read back with
   * `col(name)`. A dotted path such as `meta.userId` would be written as a literal column name but read back as nested
   * field access, so such an index can never be built or queried.
   *
   * `SchemaHelper.fieldExists` resolves dotted paths, so without this guard the configuration is accepted and persisted
   * to `metadata.json`, and only fails later during `update` with an opaque Spark analysis error.
   *
   * This restriction applies to the indexed value column only. Temporal timestamp columns may be nested, because they
   * are never persisted under their own name.
   *
   * @param column
   *   the candidate index column name
   * @param indexType
   *   the index type, used to build the error message
   * @throws IllegalArgumentException
   *   if the column is a nested path
   */
  protected def requireTopLevelIndexColumn(column: String, indexType: String): Unit =
    require(
      !column.contains("."),
      s"Nested column '$column' cannot be used as the value column for $indexType. Index columns " +
        "are stored under their own name, so only top-level columns are supported. Project the " +
        "nested field to a top-level column before indexing.")

  /**
   * Derives a transient working column name that cannot collide with columns already in play.
   *
   * Build-time projections need scratch column names. Because any user column name is legal, a fixed literal such as
   * `_ariadne_ts` can collide with a genuine indexed column and make the subsequent reference ambiguous. Underscores
   * are appended until the name is free. These names are never persisted.
   *
   * @param base
   *   the preferred name
   * @param taken
   *   names that must be avoided
   * @return
   *   a name not present in `taken`
   */
  private def uniqueWorkingName(base: String, taken: Set[String]): String =
    Iterator.iterate(base)(_ + "_").find(candidate => !taken.contains(candidate)).getOrElse(base)

  /**
   * Returns the index type already registered for a column, if any.
   *
   * @param column
   *   the column name to look up
   * @return
   *   the label of the registered index type, or `None` if the column is not indexed
   */
  private def registeredIndexTypeFor(column: String): Option[String] =
    if (metadata.indexes.contains(column)) Some("regular")
    else if (metadata.computed_indexes.containsKey(column)) Some("computed")
    else if (metadata.exploded_field_indexes.asScala.exists(_.as_column == column)) Some("exploded field")
    else if (metadata.bloom_indexes.asScala.exists(_.column == column)) Some("bloom")
    else if (metadata.temporal_indexes.asScala.exists(_.column == column)) Some("temporal")
    else if (metadata.range_indexes.asScala.exists(_.column == column)) Some("range")
    else None

  /**
   * Enforces that a column carries at most one index type.
   *
   * Checking every type in one place keeps the matrix symmetric by construction: a column carrying two index types can
   * produce wrong results at query time rather than an error, and per-method lists of checks make detection depend on
   * registration order wherever an entry is missing. A new index type therefore cannot reintroduce a one-directional
   * gap.
   *
   * Callers must invoke this only after confirming the column is not already registered as `newType`, so that re-adding
   * the same index type remains idempotent.
   *
   * @param column
   *   the column being indexed
   * @param newType
   *   the label of the index type being added, used in the error message
   * @throws IllegalArgumentException
   *   if the column already carries a different index type
   */
  protected def requireColumnNotAlreadyIndexed(column: String, newType: String): Unit =
    registeredIndexTypeFor(column).filter(_ != newType).foreach { existing =>
      throw new IllegalArgumentException(
        s"Column '$column' is already ${indefiniteArticle(existing)} $existing index. " +
          s"A column cannot be both ${indefiniteArticle(newType)} $newType index " +
          s"and ${indefiniteArticle(existing)} $existing index.")
    }

  /** Returns the English indefinite article for an index type label. */
  private def indefiniteArticle(label: String): String = if (label.startsWith("e")) "an" else "a"

  /**
   * Computes file sizes in bytes for the given files using the Hadoop FileSystem.
   *
   * Files that cannot be found or read are silently skipped with a warning log.
   *
   * @param files
   *   set of fully-qualified file paths to measure
   * @return
   *   map from file path to size in bytes; missing/unreadable files are omitted
   */
  protected def getFileSizes(files: Set[String]): Map[String, Long] = {
    logger.warn(s"Computing file sizes for ${files.size} files")
    files.toSeq.flatMap { f =>
      try {
        val path = new org.apache.hadoop.fs.Path(f)
        Some(f -> fs.getFileStatus(path).getLen)
      } catch {
        case _: java.io.FileNotFoundException =>
          logger.warn(s"File not found when computing size, skipping: $f")
          None
        case e: java.io.IOException =>
          logger.warn(s"I/O error computing file size for $f: ${e.getMessage}, skipping")
          None
        case e: Exception =>
          logger.warn(s"Unexpected error computing file size for $f: ${e.getMessage}", e)
          None
      }
    }.toMap
  }

  /**
   * Hadoop root path for large index Delta tables (`{storagePath}/large_indexes/`).
   */
  protected def largeIndexesFilePath: Path =
    new Path(storagePath, "large_indexes")

  /** Hadoop path for the main index Delta table (`{storagePath}/index/`). */
  protected def indexFilePath: Path = new Path(storagePath, "index")

  /**
   * Returns the set of column names that have large index Delta tables.
   *
   * @return
   *   Set of column names with large index storage
   */
  protected def largeIndexColumns: Set[String] =
    if (!exists(largeIndexesFilePath)) Set.empty
    else {
      fs.listStatus(largeIndexesFilePath)
        .filter(_.isDirectory)
        .map(_.getPath)
        .filter(path => exists(path) && DeltaTable.isDeltaTable(spark, path.toString))
        .map(_.getName)
        .toSet
    }

  /**
   * Loads a large index Delta table for a specific column. Large indexes store data in exploded (filename, value) row
   * form rather than as arrays.
   *
   * @param colName
   *   The column name
   * @return
   *   DataFrame with (filename, colName) rows, or None if no large index exists
   */
  private[ariadne] def loadLargeIndex(colName: String): Option[DataFrame] = {
    val columnPath = new Path(largeIndexesFilePath, colName)
    try {
      if (
        exists(columnPath) && DeltaTable
          .isDeltaTable(spark, columnPath.toString)
      )
        Some(spark.read.format("delta").load(columnPath.toString))
      else None
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to load large index for column '$colName' in index '$name': ${e.getMessage}")
        None
    }
  }

  /**
   * Hadoop path for the staging Delta table (`{storagePath}/staging/`).
   *
   * The staging table accumulates batch results during [[Index.update update]] and is merged into the main index by
   * [[consolidateStaging]].
   */
  protected def stagingFilePath: Path = new Path(storagePath, "staging")

  private def migrationDelta(path: Path, tableName: String): Option[DeltaTable] =
    if (!exists(path)) {
      None
    } else if (!DeltaTable.isDeltaTable(spark, path.toString)) {
      throw new StorageMigrationException(s"$tableName for index '$name' is not a valid Delta table at $path")
    } else {
      Some(DeltaTable.forPath(spark, path.toString))
    }

  private def declaredStorageVersion: Int =
    Option(metadata.storage_format_version).map(_.intValue()).getOrElse(StorageFormat.Alpha37StorageVersion)

  private def validateDeclaredVersions(): Unit = {
    val configuredStorageColumns =
      metadata.indexes.asScala.toSet ++
        metadata.computed_indexes.keySet().asScala ++
        metadata.exploded_field_indexes.asScala.map(_.as_column) ++
        metadata.temporal_indexes.asScala.map(_.column) ++
        metadata.bloom_indexes.asScala.map(_.column) ++
        metadata.range_indexes.asScala.map(_.column) ++
        metadata.auto_bloom_indexes.asScala
    val reservedColumns = configuredStorageColumns.intersect(ReservedStagingColumnNames)
    if (reservedColumns.nonEmpty) {
      throw new StorageMigrationException(
        s"Index '$name' uses reserved staging column(s): ${reservedColumns.toSeq.sorted.mkString(", ")}")
    }
    Option(metadata.metadata_version).foreach { version =>
      if (version > StorageFormat.CurrentMetadataVersion) {
        throw new UnsupportedMetadataVersionException(version, StorageFormat.CurrentMetadataVersion)
      }
    }
    val storageVersion = declaredStorageVersion
    if (storageVersion < StorageFormat.Alpha37StorageVersion) {
      throw new StorageMigrationException(
        s"Index '$name' declares invalid storage format version $storageVersion; " +
          s"the compatibility floor is ${StorageFormat.Alpha37StorageVersion}")
    }
    if (storageVersion > StorageFormat.CurrentStorageVersion) {
      throw new UnsupportedStorageFormatVersionException(storageVersion, StorageFormat.CurrentStorageVersion)
    }
  }

  /**
   * Ensures the index uses the current physical storage format.
   *
   * Current indexes return without acquiring a lock. Unversioned or older indexes acquire the update lock, refresh
   * metadata to close the detection/acquisition race, and run ordered idempotent migrations before the operation
   * continues.
   */
  private[ariadne] def ensureStorageReady(): Unit = {
    validateDeclaredVersions()
    if (
      metadata.storage_format_version == null ||
      declaredStorageVersion < StorageFormat.CurrentStorageVersion ||
      metadata.metadata_version == null ||
      metadata.metadata_version != StorageFormat.CurrentMetadataVersion
    ) {
      val lock = IndexLock(updateLockPath, name)
      val correlationId = UUID.randomUUID().toString
      lock.acquire(correlationId)
      try {
        ensureStorageReadyUnderLock(lock, correlationId, refresh = true)
      } finally {
        lock.release(correlationId)
      }
    }
  }

  /**
   * Runs migration preflight while the caller holds the update lock.
   *
   * @param refresh
   *   whether to reload metadata after lock acquisition
   */
  protected def ensureStorageReadyUnderLock(lock: IndexLock, correlationId: String, refresh: Boolean): Unit =
    withMigrationHeartbeat(lock, correlationId) { checkHeartbeat =>
      if (refresh) refreshMetadata()
      validateDeclaredVersions()
      checkHeartbeat()

      val initialStorageVersion = declaredStorageVersion
      val fileSizeMigrationNeeded =
        initialStorageVersion < StorageFormat.FileSizeStorageVersion || needsFileSizeMigration
      val explodedMigrationNeeded =
        initialStorageVersion < StorageFormat.ExplodedFieldStorageVersion || needsExplodedFieldMigration

      if (fileSizeMigrationNeeded) migrateFileSizeColumns(checkHeartbeat)
      checkHeartbeat()
      verifyFileSizeColumns()
      if (explodedMigrationNeeded) migrateExplodedFieldColumns()
      checkHeartbeat()
      verifyExplodedFieldColumns()

      // Evaluated after the exploded field migration because it inspects storage column names, which that migration
      // renames.
      val autoBloomBackfillNeeded =
        initialStorageVersion < StorageFormat.AutoBloomBackfillStorageVersion || needsAutoBloomBackfill
      if (autoBloomBackfillNeeded) migrateAutoBloomFilters(checkHeartbeat)
      checkHeartbeat()
      verifyAutoBloomFilters()

      val versionChanged =
        metadata.metadata_version == null ||
          metadata.metadata_version != StorageFormat.CurrentMetadataVersion ||
          metadata.storage_format_version == null ||
          metadata.storage_format_version != StorageFormat.CurrentStorageVersion
      if (versionChanged || fileSizeMigrationNeeded || explodedMigrationNeeded || autoBloomBackfillNeeded) {
        val previousMetadataVersion = metadata.metadata_version
        val previousStorageVersion = metadata.storage_format_version
        try {
          metadata.metadata_version = StorageFormat.CurrentMetadataVersion
          metadata.storage_format_version = StorageFormat.CurrentStorageVersion
          writeMetadata(metadata)
        } catch {
          case e: Exception =>
            metadata.metadata_version = previousMetadataVersion
            metadata.storage_format_version = previousStorageVersion
            throw e
        }
        logger.warn(
          s"Storage migration preflight completed for index '$name': " +
            s"metadata=${StorageFormat.CurrentMetadataVersion}, storage=${StorageFormat.CurrentStorageVersion}")
      }
    }

  private[ariadne] def withMigrationHeartbeat(lock: IndexLock, correlationId: String)(
      body: (() => Unit) => Unit): Unit = {
    val heartbeatFailure = new AtomicReference[Throwable]()
    val scheduler =
      Executors.newSingleThreadScheduledExecutor { runnable =>
        val thread = new Thread(runnable, s"ariadne-storage-migration-$name")
        thread.setDaemon(true)
        thread
      }
    val intervalSeconds = math.max(1L, lockTimeout / 3L)
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit =
          try {
            lock.refreshOrThrow(correlationId)
          } catch {
            case e: Throwable =>
              logger.warn(s"Storage migration lock heartbeat failed for index '$name': ${e.getMessage}", e)
              heartbeatFailure.compareAndSet(null, e)
          }
      },
      0L,
      intervalSeconds,
      TimeUnit.SECONDS)

    def checkHeartbeat(): Unit =
      Option(heartbeatFailure.get()).foreach { failure =>
        throw new StorageMigrationException(s"Storage migration lock heartbeat failed for index '$name'", failure)
      }

    try {
      body(() => checkHeartbeat())
      checkHeartbeat()
    } finally {
      scheduler.shutdown()
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          scheduler.shutdownNow()
          Thread.currentThread().interrupt()
      }
    }
  }

  private def needsFileSizeMigration: Boolean =
    Seq(indexFilePath -> "main table", stagingFilePath -> "staging table").exists { case (path, tableName) =>
      migrationDelta(path, tableName).exists { table =>
        !table.toDF.columns.contains("file_size") || table.toDF.where(col("file_size").isNull).limit(1).count() > 0
      }
    }

  private def migrateFileSizeColumns(checkHeartbeat: () => Unit): Unit = {
    Seq(indexFilePath -> "main table", stagingFilePath -> "staging table").foreach { case (path, tableName) =>
      migrationDelta(path, tableName).foreach { table =>
        if (!table.toDF.columns.contains("file_size")) {
          logger.warn(s"Adding file_size to $tableName for index '$name'")
          checkHeartbeat()
          // Evolve the schema by appending an empty, schema-widened DataFrame with mergeSchema enabled. This resolves
          // through Delta's path-based DataSource writer rather than the analyzer's ResolveRelations rule, so it never
          // forces HiveExternalCatalog initialization (which fails with "null path" on Synapse). Works identically on
          // Delta 3.2 (Spark 3.5) and Delta 4.1 (Spark 4.1). Zero rows are written; only the table schema evolves.
          val evolvedSchema = table.toDF.schema.add(StructField("file_size", LongType, nullable = true))
          val emptyWithFileSize = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], evolvedSchema)
          emptyWithFileSize.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(path.toString)
          checkHeartbeat()
        }
      }

      migrationDelta(path, tableName).foreach { table =>
        import spark.implicits._
        val pendingFiles =
          table.toDF
            .where(col("file_size").isNull)
            .select("filename")
            .distinct()
        val serializableConfiguration = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
        val sizeResults =
          pendingFiles
            .as[String]
            .mapPartitions { filenames =>
              filenames.map { filename =>
                try {
                  val path = new Path(filename)
                  val length = path.getFileSystem(serializableConfiguration.value).getFileStatus(path).getLen
                  (filename, java.lang.Long.valueOf(length), null: String)
                } catch {
                  case _: java.io.FileNotFoundException =>
                    (filename, null: java.lang.Long, "file not found")
                  case e: java.io.IOException =>
                    (filename, null: java.lang.Long, s"I/O error: ${e.getMessage}")
                  case NonFatal(e) =>
                    val message = Option(e.getMessage).getOrElse("no message")
                    (filename, null: java.lang.Long, s"${e.getClass.getSimpleName}: $message")
                }
              }
            }
            .toDF("filename", "file_size", "error")
            .persist(StorageLevel.DISK_ONLY)
        try {
          checkHeartbeat()
          val failures =
            sizeResults
              .where(col("error").isNotNull)
              .select("filename", "error")
              .limit(1)
              .collect()
          checkHeartbeat()
          if (failures.nonEmpty) {
            throw new StorageMigrationException(
              s"Cannot migrate file_size for index '$name': source file '${failures.head.getString(0)}' " +
                s"is missing or unreadable (${failures.head.getString(1)})")
          }
          checkHeartbeat()
          table
            .as("target")
            .merge(sizeResults.select("filename", "file_size").as("source"), "target.filename = source.filename")
            .whenMatched()
            .update(Map("file_size" -> col("source.file_size")))
            .execute()
          checkHeartbeat()
        } finally {
          sizeResults.unpersist()
        }
      }
    }

    migrationDelta(indexFilePath, "main table").foreach { table =>
      val result = table.toDF.agg(sum("file_size")).head()
      metadata.total_indexed_file_size = if (result.isNullAt(0)) 0L else result.getLong(0)
    }
  }

  private def verifyFileSizeColumns(): Unit =
    Seq(indexFilePath -> "main table", stagingFilePath -> "staging table").foreach { case (path, tableName) =>
      migrationDelta(path, tableName).foreach { table =>
        val df = table.toDF
        if (!df.columns.contains("file_size") || df.schema("file_size").dataType != LongType) {
          throw new StorageMigrationException(s"$tableName for index '$name' does not have BIGINT file_size")
        }
        if (df.where(col("file_size").isNull).limit(1).count() > 0) {
          throw new StorageMigrationException(s"$tableName for index '$name' still contains null file_size values")
        }
      }
    }

  private def needsExplodedFieldMigration: Boolean = {
    val mappings = metadata.exploded_field_indexes.asScala.toSeq
    if (mappings.isEmpty) {
      false
    } else {
      val tableNeedsMigration =
        Seq(indexFilePath -> "main table", stagingFilePath -> "staging table").exists { case (path, tableName) =>
          migrationDelta(path, tableName).exists { table =>
            val columns = table.toDF.columns.toSet
            mappings.exists(mapping =>
              columns.contains(mapping.array_column) &&
                !columns.contains(mapping.as_column) &&
                !metadata.indexes.contains(mapping.array_column) &&
                mapping.array_column != mapping.as_column)
          }
        }
      val largeIndexNeedsMigration =
        mappings.exists(mapping =>
          mapping.array_column != mapping.as_column &&
            !metadata.indexes.contains(mapping.array_column) &&
            exists(new Path(largeIndexesFilePath, mapping.array_column)))
      tableNeedsMigration || largeIndexNeedsMigration
    }
  }

  /**
   * Migrates pre-0.1.1 indexes that stored exploded field columns under `array_column` names to the current `as_column`
   * naming convention.
   *
   * Detection: reads the main and staging Delta table schemas and checks whether any
   * `ExplodedFieldMapping.array_column` appears as a column while the corresponding `as_column` does not. When this
   * pattern is found, each table is rewritten with renamed columns, and any large-index directories named by
   * `array_column` are similarly migrated.
   *
   * This method is idempotent and is invoked only by storage migration preflight while the update lock is held.
   */
  protected def migrateExplodedFieldColumns(): Unit = {
    val mappings = metadata.exploded_field_indexes.asScala.toSeq
    val ambiguousArrays =
      mappings
        .groupBy(_.array_column)
        .collect {
          case (arrayColumn, values) if values.size > 1 =>
            arrayColumn
        }
        .toSet

    def migrateTable(path: Path, tableName: String): Unit =
      migrationDelta(path, tableName).foreach { table =>
        val schema = table.toDF.schema.fieldNames.toSet
        val columnsToRename =
          mappings.filter(m =>
            schema.contains(m.array_column) &&
              !schema.contains(m.as_column) &&
              !metadata.indexes.contains(m.array_column) &&
              m.array_column != m.as_column)
        if (columnsToRename.exists(m => ambiguousArrays.contains(m.array_column))) {
          throw new StorageMigrationException(
            s"$tableName for index '$name' has an ambiguous legacy exploded column mapping")
        }
        mappings.foreach { mapping =>
          if (
            mapping.array_column != mapping.as_column &&
            schema.contains(mapping.array_column) &&
            schema.contains(mapping.as_column) &&
            !metadata.indexes.contains(mapping.array_column)
          ) {
            throw new StorageMigrationException(
              s"$tableName for index '$name' contains both legacy '${mapping.array_column}' " +
                s"and current '${mapping.as_column}' columns without a regular '${mapping.array_column}' index")
          }
          if (
            mapping.array_column != mapping.as_column &&
            schema.contains(mapping.array_column) &&
            !schema.contains(mapping.as_column) &&
            metadata.indexes.contains(mapping.array_column)
          ) {
            throw new StorageMigrationException(
              s"$tableName for index '$name' has regular '${mapping.array_column}' data but is missing " +
                s"exploded alias '${mapping.as_column}'; run a column backfill before migration")
          }
        }
        if (columnsToRename.nonEmpty) {
          logger.warn(
            s"Migrating exploded field columns in $tableName for index '$name': " +
              columnsToRename.map(m => s"${m.array_column} -> ${m.as_column}").mkString(", "))
          val migrated =
            columnsToRename.foldLeft(table.toDF) { case (df, mapping) =>
              df.withColumnRenamed(mapping.array_column, mapping.as_column)
            }
          migrated.write
            .format("delta")
            .option("overwriteSchema", "true")
            .mode("overwrite")
            .save(path.toString)
        }
      }

    migrateTable(indexFilePath, "main table")
    migrateTable(stagingFilePath, "staging table")

    mappings.foreach { mapping =>
      val oldPath = new Path(largeIndexesFilePath, mapping.array_column)
      val newPath = new Path(largeIndexesFilePath, mapping.as_column)
      if (
        exists(oldPath) &&
        mapping.array_column != mapping.as_column &&
        !metadata.indexes.contains(mapping.array_column)
      ) {
        if (exists(newPath)) {
          throw new StorageMigrationException(
            s"Index '$name' contains both legacy and current large index paths for '${mapping.as_column}'")
        }
        logger.warn(s"Renaming large index directory: ${mapping.array_column} -> ${mapping.as_column}")
        if (!fs.rename(oldPath, newPath)) {
          throw new StorageMigrationException(s"Failed to rename legacy large index directory $oldPath to $newPath")
        }
      }
    }
  }

  private def verifyExplodedFieldColumns(): Unit = {
    val mappings = metadata.exploded_field_indexes.asScala.toSeq
    Seq(indexFilePath -> "main table", stagingFilePath -> "staging table").foreach { case (path, tableName) =>
      migrationDelta(path, tableName).foreach { table =>
        val columns = table.toDF.columns.toSet
        mappings.foreach { mapping =>
          if (
            mapping.array_column != mapping.as_column &&
            columns.contains(mapping.array_column) &&
            !metadata.indexes.contains(mapping.array_column)
          ) {
            throw new StorageMigrationException(
              s"$tableName for index '$name' still contains legacy exploded column '${mapping.array_column}'")
          }
        }
      }
    }
    mappings.foreach { mapping =>
      if (
        mapping.array_column != mapping.as_column &&
        !metadata.indexes.contains(mapping.array_column) &&
        exists(new Path(largeIndexesFilePath, mapping.array_column))
      ) {
        throw new StorageMigrationException(
          s"Index '$name' still contains legacy large index path '${mapping.array_column}'")
      }
    }
  }

  /**
   * Returns files that hold at least one non-null value in the large index for `colName` but carry no auto-bloom filter
   * in the main table.
   *
   * A file whose large-index values are all null contributes nothing to a filter, so it is not reported as missing.
   *
   * The main table holds one row per file while the large index holds one row per value, so the file-side check runs
   * first and returns `None` when every file already carries a filter. This keeps the large index out of the path taken
   * by every mutating operation once a backfill has completed.
   *
   * @param indexDf
   *   the main index DataFrame
   * @param colName
   *   the storage column name
   * @return
   *   a single-column `filename` DataFrame, or `None` when no file can be missing a filter
   */
  private def filesMissingAutoBloom(indexDf: DataFrame, colName: String): Option[DataFrame] = {
    val bloomColumn = autoBloomColumnPrefix + colName
    val unfiltered =
      if (indexDf.columns.contains(bloomColumn))
        indexDf.where(col(bloomColumn).isNull).select(col("filename")).distinct()
      else indexDf.select(col("filename")).distinct()

    if (unfiltered.limit(1).count() == 0) None
    else
      loadLargeIndex(colName).map { largeDf =>
        largeDf
          .where(autoBloomValueColumn(colName).isNotNull)
          .join(unfiltered, Seq("filename"), "left_semi")
          .select(col("filename"))
          .distinct()
      }
  }

  private def autoBloomBackfillColumns: Set[String] =
    autoBloomEligibleColumns.intersect(largeIndexColumns)

  private def needsAutoBloomBackfill: Boolean = {
    val candidates = autoBloomBackfillColumns
    candidates.nonEmpty && migrationDelta(indexFilePath, "main table").exists { table =>
      val df = table.toDF
      candidates.exists { colName =>
        !metadata.auto_bloom_indexes.contains(colName) ||
        !df.columns.contains(autoBloomColumnPrefix + colName) ||
        filesMissingAutoBloom(df, colName).exists(_.limit(1).count() > 0)
      }
    }
  }

  /**
   * Builds auto-bloom filters for columns that already have a `large_indexes/` table but no filter in the main index.
   *
   * Filters are folded from the large index rows themselves. A file's values for a column are stored either entirely
   * inline or entirely in the large index, so the large index alone is a complete value source for the files it covers.
   *
   * Metadata is written only after every column has been backfilled, so a failure part way through leaves the storage
   * version unchanged and the next preflight repeats the work.
   *
   * @param checkHeartbeat
   *   callback that refreshes the update lock and aborts if ownership was lost
   */
  protected def migrateAutoBloomFilters(checkHeartbeat: () => Unit): Unit = {
    val candidates = autoBloomBackfillColumns.toSeq.sorted
    if (candidates.nonEmpty && exists(indexFilePath)) {
      val fpr = autoBloomFpr
      logger.warn(s"Backfilling auto-bloom filters for index '$name': ${candidates.mkString(", ")}")
      candidates.foreach { colName =>
        val bloomColumn = autoBloomColumnPrefix + colName
        checkHeartbeat()

        migrationDelta(indexFilePath, "main table").foreach { table =>
          if (!table.toDF.columns.contains(bloomColumn)) {
            logger.warn(s"Adding $bloomColumn to main table for index '$name'")
            // Evolve the schema by appending an empty, schema-widened DataFrame with mergeSchema enabled. This resolves
            // through Delta's path-based DataSource writer rather than the analyzer's ResolveRelations rule, so it
            // never forces HiveExternalCatalog initialization (which fails with "null path" on Synapse). Zero rows are
            // written; only the table schema evolves.
            val evolvedSchema = table.toDF.schema.add(StructField(bloomColumn, BinaryType, nullable = true))
            val emptyWithBloom = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], evolvedSchema)
            emptyWithBloom.write
              .format("delta")
              .mode("append")
              .option("mergeSchema", "true")
              .save(indexFilePath.toString)
          }
        }
        checkHeartbeat()

        migrationDelta(indexFilePath, "main table").foreach { table =>
          filesMissingAutoBloom(table.toDF, colName).foreach { pendingFiles =>
            loadLargeIndex(colName).foreach { largeDf =>
              val pendingRows = largeDf.join(pendingFiles, Seq("filename"), "left_semi")
              val filters = buildStreamingBloomColumn(pendingRows, autoBloomValueColumn(colName), bloomColumn, fpr)
              checkHeartbeat()
              table
                .as("target")
                .merge(filters.as("source"), "target.filename = source.filename")
                .whenMatched()
                .update(Map(bloomColumn -> col(s"source.$bloomColumn")))
                .execute()
              checkHeartbeat()
            }
          }
        }
      }

      val newlyRegistered = candidates.filterNot(metadata.auto_bloom_indexes.contains)
      if (newlyRegistered.nonEmpty) {
        newlyRegistered.foreach(metadata.auto_bloom_indexes.add)
        writeMetadata(metadata)
        logger.warn(s"Registered auto-bloom columns for index '$name': ${newlyRegistered.mkString(", ")}")
      }
    }
  }

  private def verifyAutoBloomFilters(): Unit = {
    val candidates = autoBloomBackfillColumns
    if (candidates.nonEmpty) {
      migrationDelta(indexFilePath, "main table").foreach { table =>
        val df = table.toDF
        candidates.foreach { colName =>
          val bloomColumn = autoBloomColumnPrefix + colName
          if (!metadata.auto_bloom_indexes.contains(colName)) {
            throw new StorageMigrationException(
              s"Index '$name' has a large index for '$colName' that is not registered as auto-bloom")
          }
          if (!df.columns.contains(bloomColumn) || df.schema(bloomColumn).dataType != BinaryType) {
            throw new StorageMigrationException(
              s"Main table for index '$name' does not have a binary $bloomColumn column")
          }
          if (filesMissingAutoBloom(df, colName).exists(_.limit(1).count() > 0)) {
            throw new StorageMigrationException(
              s"Main table for index '$name' still has large index files without an auto-bloom filter for '$colName'")
          }
        }
      }
    }
  }

  /**
   * Returns the set of all storage column names across regular, computed, exploded-field, and temporal index types.
   *
   * This is used internally to determine which columns to check for large-index separation and to build aggregation
   * expressions.
   *
   * @return
   *   set of column names used for index storage (excludes bloom, range, and auto-bloom)
   */
  protected def storageColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.as_column).toSet ++
      metadata.temporal_indexes.asScala.map(_.column).toSet

  /**
   * Returns the set of range index storage column names (each prefixed with `range_`).
   *
   * @return
   *   set of prefixed column names (e.g., `range_event_date`)
   */
  protected def rangeStorageColumns: Set[String] =
    metadata.range_indexes.asScala.map(c => s"range_${c.column}").toSet

  /**
   * Identifies which files contribute too many distinct values to a column to be stored inline.
   *
   * A `(file, column)` pair is "large" when the file contributes at least `largeIndexLimit` distinct values to that
   * column. Those values are streamed into `large_indexes/{column}` as individual rows instead of being collected into
   * a per-file array, and the inline array column is left `null`.
   *
   * Classification is deliberately based on the '''distinct''' value count rather than the row count, so a file with
   * many duplicate rows but few distinct values stays inline.
   *
   * @param byColumn
   *   map of column name to the set of filenames that are large for that column; columns with no large files are
   *   omitted
   */
  protected case class LargeFileClassification(byColumn: Map[String, Set[String]]) {

    /**
     * Returns the filenames that are large for `column`.
     *
     * @param column
     *   the storage column name
     * @return
     *   set of filenames, empty when the column has no large files
     */
    def filesFor(column: String): Set[String] = byColumn.getOrElse(column, Set.empty)

    /**
     * Returns the columns that have at least one large file.
     *
     * @return
     *   set of column names
     */
    def columns: Set[String] = byColumn.keySet

    /**
     * Builds a predicate matching rows whose `filename` is large for `column`.
     *
     * @param column
     *   the storage column name
     * @return
     *   a boolean column; literal `false` when the column has no large files
     */
    def isLarge(column: String): Column = {
      val files = filesFor(column)
      if (files.isEmpty) lit(false) else col("filename").isin(files.toSeq: _*)
    }

    /**
     * Masks `expr` to `null` for rows belonging to a file that is large for `column`.
     *
     * Nulling the value before aggregation is what keeps the giant array from ever being built: `collect_set` skips
     * nulls, so a large file aggregates to an empty array which is then replaced with `null`.
     *
     * @param column
     *   the storage column name
     * @param expr
     *   the value expression to mask
     * @return
     *   the masked expression, or `expr` unchanged when the column has no large files
     */
    def mask(column: String, expr: Column): Column =
      if (filesFor(column).isEmpty) expr else when(isLarge(column), lit(null)).otherwise(expr)

    /**
     * Replaces the aggregated array in `df` with `null` for every file that is large for `column`.
     *
     * @param df
     *   the aggregated DataFrame containing `filename` and `column`
     * @param column
     *   the storage column name
     * @return
     *   the DataFrame with large files' arrays nulled; unchanged when the column has no large files
     */
    def nullOutLargeArrays(df: DataFrame, column: String): DataFrame =
      if (filesFor(column).isEmpty || !df.columns.contains(column)) df
      else
        df.withColumn(column, when(isLarge(column), lit(null).cast(df.schema(column).dataType)).otherwise(col(column)))
  }

  /**
   * Classifies each `(file, column)` pair as large or inline using pre-flight distinct counts.
   *
   * @param analyses
   *   per-file analyses produced by [[analyzeFiles]]
   * @return
   *   the resulting [[LargeFileClassification]]; empty when no pair reaches `largeIndexLimit`
   */
  protected def classifyLargeFiles(analyses: Seq[FileAnalysis]): LargeFileClassification = {
    val limit = largeIndexLimit
    val byColumn =
      storageColumns.toSeq
        .map(column => column -> analyses.filter(_.distinctCounts.getOrElse(column, 0L) >= limit).map(_.filename).toSet)
        .toMap
        .filter { case (_, files) => files.nonEmpty }

    if (byColumn.nonEmpty) {
      logger.warn(
        s"Large index classification for '$name' (limit=$limit): " +
          byColumn.map { case (c, f) => s"$c=${f.size} file(s)" }.mkString(", "))
    }
    LargeFileClassification(byColumn)
  }

  /**
   * Produces the `(filename, value)` rows for an index column, one row per occurrence.
   *
   * Rows are produced directly from the source data without aggregating into arrays. This is the value source for both
   * `large_indexes/{column}` and auto-bloom filters. Callers are responsible for any `distinct` or filename filtering
   * they need.
   *
   * Regular and computed indexes project the column directly; exploded-field indexes explode the configured nested
   * path; temporal indexes reduce to one `(value, max_ts)` struct per distinct value.
   *
   * @param df
   *   the base DataFrame with a `filename` column, computed indexes applied, and all source columns present
   * @param column
   *   the storage column name
   * @return
   *   `Some` DataFrame of `filename` plus `column`, or `None` when `column` is not a known storage column
   */
  protected def columnValueRows(df: DataFrame, column: String): Option[DataFrame] = {
    val explodedConfig = metadata.exploded_field_indexes.asScala.find(_.as_column == column)
    val temporalConfig = metadata.temporal_indexes.asScala.find(_.column == column)
    val isRegular =
      metadata.indexes.contains(column) || metadata.computed_indexes.containsKey(column)

    if (explodedConfig.isDefined) {
      val config = explodedConfig.get
      Some(df.select(col("filename"), explode(col(s"${config.array_column}.${config.field_path}")).alias(column)))
    } else if (temporalConfig.isDefined) {
      val config = temporalConfig.get
      val taken = Set("filename", config.column)
      val tsAlias = uniqueWorkingName("_ariadne_ts", taken)
      val maxTsAlias = uniqueWorkingName("_ariadne_max_ts", taken)
      Some(
        df
          .select(col("filename"), col(config.column), col(config.timestamp_column).alias(tsAlias))
          .groupBy("filename", config.column)
          .agg(max(col(tsAlias)).alias(maxTsAlias))
          .select(col("filename"), struct(col(config.column).as("value"), col(maxTsAlias).as("max_ts")).alias(column)))
    } else if (isRegular) {
      Some(df.select(col("filename"), col(column)))
    } else {
      None
    }
  }

  /**
   * Returns the expression selecting the bloom-hashable scalar out of a [[columnValueRows]] row.
   *
   * Every column type but temporal yields the value directly. Temporal rows carry a `(value, max_ts)` struct, so the
   * filter is folded over the `value` field alone to match the bare scalars that queries probe with.
   *
   * @param column
   *   the storage column name, as produced by [[columnValueRows]]
   * @return
   *   the column expression to hash into the filter
   */
  protected def autoBloomValueColumn(column: String): Column =
    if (metadata.temporal_indexes.asScala.exists(_.column == column)) col(column).getField("value")
    else col(column)

  /**
   * Case class to hold file analysis results for batching decisions.
   *
   * @param filename
   *   The name of the file
   * @param distinctCounts
   *   Map of column name to distinct value count for that file
   * @param maxDistinctCount
   *   Maximum distinct count across all indexed columns
   */
  case class FileAnalysis(filename: String, distinctCounts: Map[String, Long], maxDistinctCount: Long)

  /**
   * Performs pre-flight analysis on unindexed files to determine optimal batching strategy.
   *
   * Reads the source files, applies computed indexes, and counts distinct values per indexed column per file. The
   * resulting [[FileAnalysis]] objects are used by [[createOptimalBatches]] to group files into batches that stay under
   * the `largeIndexLimit`.
   *
   * If no storage columns are configured (e.g., only bloom or range indexes), returns trivial analyses with zero
   * distinct counts.
   *
   * Each column is counted on the same basis its builder uses, because these counts also drive [[classifyLargeFiles]].
   * Regular, computed, and temporal columns are counted before exploded fields are applied: `applyExplodedFields` uses
   * an inner `explode`, which drops rows whose array is null or empty and would undercount every other column. Temporal
   * columns add one for a present null value, matching the `collect_set` of `struct(value, max_ts)` in
   * [[buildTemporalIndexes]], which retains a struct for the null group.
   *
   * @note
   *   This method calls `.collect()` to bring per-file distinct counts to the driver. For indexes covering millions of
   *   files with many indexed columns, the collected result set can be large enough to cause driver OOM. Consider
   *   limiting the number of files analyzed per call or increasing driver memory.
   *
   * @param files
   *   set of file paths to analyze
   * @return
   *   sequence of [[FileAnalysis]] objects with per-column distinct counts; empty if `files` is empty
   */
  protected def analyzeFiles(files: Set[String]): Seq[FileAnalysis] =
    if (files.isEmpty) Seq.empty
    else {
      val startTime = System.currentTimeMillis()
      logger.warn(s"Performing pre-flight analysis on ${files.size} files")

      val allStorageColumns = storageColumns
      if (allStorageColumns.isEmpty) {
        files.map(f => FileAnalysis(f, Map.empty, 0L)).toSeq
      } else {
        val baseDf = createBaseDataFrame(files)
        val withComputedIndexes = applyComputedIndexes(baseDf)
        val withFilename = addFilenameColumn(withComputedIndexes, files)

        val explodedColumns = metadata.exploded_field_indexes.asScala.map(_.as_column).toSet
        val temporalColumns = metadata.temporal_indexes.asScala.map(_.column).toSet
        val directColumns = (allStorageColumns -- explodedColumns).toSeq

        // Counted on the unexploded rows so a null or empty array elsewhere cannot drop rows here.
        val directCounts =
          if (directColumns.isEmpty) None
          else {
            val exprs =
              directColumns.map { colName =>
                val distinct = countDistinct(col(colName))
                // collect_set(struct(value, max_ts)) keeps one entry for the null group.
                val expr =
                  if (temporalColumns.contains(colName))
                    distinct + max(when(col(colName).isNull, lit(1L)).otherwise(lit(0L)))
                  else distinct
                expr.alias(s"${colName}_distinct")
              }
            Some(withFilename.groupBy("filename").agg(exprs.head, exprs.tail: _*))
          }

        // Exploded columns only exist after the explode, so they need their own pass. Each config
        // is counted from its own array in isolation, mirroring buildExplodedFieldIndexes: a shared
        // applyExplodedFields plan folds an inner `explode` over every configured array, so a row
        // with one null array would be dropped before the other columns were counted.
        // `explode_outer` keeps one row per source row when an array is null or empty, so the file
        // still appears in the analysis with a count of zero (countDistinct ignores the null).
        // Without it, an index whose columns are all exploded would omit such a file entirely and
        // createOptimalBatches would never schedule it, silently skipping the file during update.
        val explodedConfigs =
          metadata.exploded_field_indexes.asScala.toSeq.filter(c => allStorageColumns.contains(c.as_column))
        val explodedTargets = explodedConfigs.map(_.as_column)
        val explodedCounts =
          explodedConfigs
            .map { explodedField =>
              withFilename
                .select("filename", explodedField.array_column)
                .withColumn(
                  "temp_exploded",
                  explode_outer(col(s"${explodedField.array_column}.${explodedField.field_path}")))
                .groupBy("filename")
                .agg(countDistinct(col("temp_exploded")).alias(s"${explodedField.as_column}_distinct"))
            }
            .reduceOption((left, right) => left.join(right, Seq("filename"), "full_outer"))

        val combined =
          (directCounts, explodedCounts) match {
            case (Some(d), Some(e)) => Some(d.join(e, Seq("filename"), "full_outer"))
            case (Some(d), None) => Some(d)
            case (None, Some(e)) => Some(e)
            case (None, None) => None
          }

        combined match {
          case None => files.map(f => FileAnalysis(f, Map.empty, 0L)).toSeq
          case Some(fileAnalysisDf) =>
            val analysisColumns = directColumns ++ explodedTargets
            val analysisResults = fileAnalysisDf.collect()

            val results =
              analysisResults.map { row =>
                val filename = row.getAs[String]("filename")
                val distinctCounts =
                  analysisColumns.map { colName =>
                    val field = s"${colName}_distinct"
                    val value = if (row.isNullAt(row.fieldIndex(field))) 0L else row.getAs[Long](field)
                    colName -> value
                  }.toMap
                val maxCount =
                  if (distinctCounts.nonEmpty) distinctCounts.values.max else 0L

                FileAnalysis(filename, distinctCounts, maxCount)
              }.toSeq

            logger.warn(
              s"Pre-flight analysis of ${files.size} files completed in ${System.currentTimeMillis() - startTime}ms")
            results
        }
      }
    }

  /**
   * Groups files into batches whose aggregate `maxDistinctCount` stays at or below `largeIndexLimit`.
   *
   * The algorithm sorts files by `maxDistinctCount` (largest first) and packs them sequentially into batches. When
   * adding a file would push the batch total past the limit, a new batch is started. Files that individually exceed the
   * limit are placed into single-file batches to guarantee progress.
   *
   * @param fileAnalyses
   *   sequence of [[FileAnalysis]] objects from [[analyzeFiles]]
   * @return
   *   sequence of file batches (each a `Set[String]` of filenames); empty if `fileAnalyses` is empty
   */
  protected def createOptimalBatches(fileAnalyses: Seq[FileAnalysis]): Seq[Set[String]] =
    if (fileAnalyses.isEmpty) Seq.empty
    else {
      val allStorageColumns = storageColumns
      if (allStorageColumns.isEmpty) {
        Seq(fileAnalyses.map(_.filename).toSet)
      } else {
        logger.warn(s"Creating optimal batches for ${fileAnalyses.size} files with largeIndexLimit=$largeIndexLimit")

        // Separate files that individually exceed the limit (will be processed individually)
        val (largeFiles, regularFiles) =
          fileAnalyses.partition(_.maxDistinctCount > largeIndexLimit)

        if (largeFiles.nonEmpty) {
          logger.warn(
            s"Found ${largeFiles.size} files that individually exceed largeIndexLimit and will be processed separately")
        }

        val batches = scala.collection.mutable.ListBuffer[Set[String]]()

        // Sort files by maxDistinctCount (largest first for better packing)
        val sortedFiles = regularFiles.sortBy(-_.maxDistinctCount)

        // Simple sequential batching - group files until sum of maxDistinctCount reaches largeIndexLimit
        var currentBatch = scala.collection.mutable.Set[String]()
        var currentBatchTotal = 0L

        for (fileAnalysis <- sortedFiles) {
          val filename = fileAnalysis.filename
          val fileMaxDistinct = fileAnalysis.maxDistinctCount

          // Check if adding this file would exceed the limit
          if (currentBatchTotal + fileMaxDistinct > largeIndexLimit) {
            // Start a new batch if current batch is not empty
            if (currentBatch.nonEmpty) {
              batches += currentBatch.toSet
              currentBatch = scala.collection.mutable.Set[String]()
              currentBatchTotal = 0L
            }
          }

          // Add file to current batch
          currentBatch += filename
          currentBatchTotal += fileMaxDistinct
        }

        // Add the final batch if it's not empty
        if (currentBatch.nonEmpty) {
          batches += currentBatch.toSet
        }

        // Add individual large files as single-file batches
        val largeBatches = largeFiles.map(fa => Set(fa.filename))

        val allBatches = batches.toSeq ++ largeBatches
        logger.warn(
          s"Created ${allBatches.size} batches: ${batches.size} regular batches + " +
            s"${largeBatches.size} large file batches")

        allBatches
      }
    }

  /**
   * Builds regular (array-aggregated) indexes for all configured regular and computed columns.
   *
   * Groups the input data by filename and collects distinct values into arrays via `collect_set`. If no regular or
   * computed indexes are configured, returns a distinct filename-only DataFrame.
   *
   * @param df
   *   the base DataFrame with `filename` column and all indexed source columns
   * @return
   *   DataFrame with `filename` plus one array column per regular/computed index
   */
  protected def buildRegularIndexes(df: DataFrame, large: LargeFileClassification): DataFrame = {
    val regularIndexes =
      metadata.indexes.asScala.toSet ++
        metadata.computed_indexes.keySet().asScala

    logger.debug(s"Building regular indexes for ${regularIndexes.size} columns: ${regularIndexes.mkString(", ")}")

    if (regularIndexes.nonEmpty) {
      // Masking before the distinct collapses a large file's values to a single null row, so the
      // giant array is never materialized on an executor. The column is nulled outright afterwards.
      val selectedDf =
        df
          .select(col("filename") +: regularIndexes.toList.map(c => large.mask(c, col(c)).alias(c)): _*)
          .distinct
      val aggExprs = regularIndexes.toList.map(colName => collect_set(col(colName)).alias(colName))
      // Safe: regularIndexes.nonEmpty guard above guarantees aggExprs.nonEmpty
      val grouped = selectedDf.groupBy("filename").agg(aggExprs.head, aggExprs.tail: _*)
      regularIndexes.foldLeft(grouped)((accumDf, colName) => large.nullOutLargeArrays(accumDf, colName))
    } else {
      df.select("filename").distinct
    }
  }

  /**
   * Builds exploded field indexes and joins them onto the result DataFrame.
   *
   * For each configured exploded field index, extracts the nested field path from the array column, explodes it,
   * collects distinct values back into an array per file, and joins the result onto `resultDf`.
   *
   * @param baseData
   *   the full base DataFrame with all source columns
   * @param resultDf
   *   the accumulating result DataFrame to join with (must have `filename`)
   * @return
   *   DataFrame with exploded field index columns joined via `full_outer`
   */
  protected def buildExplodedFieldIndexes(
      baseData: DataFrame,
      resultDf: DataFrame,
      large: LargeFileClassification): DataFrame = {
    val explodedFieldMappings = metadata.exploded_field_indexes.asScala.toSeq

    logger.debug(s"Building exploded field indexes for ${explodedFieldMappings.size} mapping(s)")

    explodedFieldMappings.foldLeft(resultDf) { (accumDf, explodedField) =>
      val explodedDf =
        baseData
          .select("filename", explodedField.array_column)
          .where(not(large.isLarge(explodedField.as_column)))
          .withColumn("temp_exploded", explode(col(s"${explodedField.array_column}.${explodedField.field_path}")))
          .groupBy("filename")
          .agg(collect_set(col("temp_exploded")).alias(explodedField.as_column))

      accumDf.join(explodedDf, Seq("filename"), "full_outer")
    }
  }

  /**
   * Builds temporal indexes storing `Array[Struct(value, max_ts)]` per file.
   *
   * For each temporal index configuration, groups by `(filename, value_column)` to find the maximum timestamp per value
   * per file, then collects the `(value, max_ts)` structs into an array per file. This enables temporal deduplication
   * at query time.
   *
   * @param df
   *   the base DataFrame with `filename`, value, and timestamp columns
   * @return
   *   DataFrame with `filename` plus one struct-array column per temporal index; filename-only DataFrame if no temporal
   *   indexes are configured
   */
  protected def buildTemporalIndexes(df: DataFrame, large: LargeFileClassification): DataFrame = {
    val temporalConfigs = metadata.temporal_indexes.asScala.toSeq
    if (temporalConfigs.isEmpty) {
      df.select("filename").distinct()
    } else {
      logger.debug(
        s"Building temporal indexes for ${temporalConfigs.size} columns: " +
          s"${temporalConfigs.map(_.column).mkString(", ")}")

      temporalConfigs.foldLeft(df.select("filename").distinct()) { (accumDf, config) =>
        // The timestamp may be a nested path such as `meta.updatedAt`. Selecting it flattens the
        // path to its leaf name, so aggregating by the original dotted path would not resolve.
        // Alias the projection to a working name and aggregate on that. The working names are
        // derived per config so they cannot collide with the indexed value column itself.
        val taken = Set("filename", config.column)
        val tsAlias = uniqueWorkingName("_ariadne_ts", taken)
        val maxTsAlias = uniqueWorkingName("_ariadne_max_ts", taken)
        val structAlias = uniqueWorkingName("_ariadne_struct", taken)

        val perFilePerValue =
          df
            .select(col("filename"), col(config.column), col(config.timestamp_column).alias(tsAlias))
            .where(not(large.isLarge(config.column)))
            .groupBy("filename", config.column)
            .agg(max(col(tsAlias)).alias(maxTsAlias))

        val structPerFile =
          perFilePerValue
            .withColumn(structAlias, struct(col(config.column).as("value"), col(maxTsAlias).as("max_ts")))
            .groupBy("filename")
            .agg(collect_set(col(structAlias)).alias(config.column))

        accumDf.join(structPerFile, Seq("filename"), "full_outer")
      }
    }
  }

  /**
   * Builds range indexes storing `Struct(min, max)` per file.
   *
   * For each range index configuration, groups by `filename` and computes the `min` and `max` of the column per file.
   * The result is a struct column named `range_{column}`.
   *
   * @param df
   *   the base DataFrame with `filename` column and range-indexed source columns
   * @return
   *   DataFrame with `filename` plus one range struct column per configured range index; filename-only DataFrame if no
   *   range indexes are configured
   */
  protected def buildRangeIndexes(df: DataFrame): DataFrame = {
    val rangeConfigs = metadata.range_indexes.asScala.toSeq
    if (rangeConfigs.isEmpty) df.select("filename").distinct()
    else {
      logger.warn(
        s"Building range indexes for ${rangeConfigs.size} columns: ${rangeConfigs.map(_.column).mkString(", ")}")

      rangeConfigs.foldLeft(df.select("filename").distinct()) { (accumDf, config) =>
        val rangeCol = s"range_${config.column}"
        val perFile =
          df
            .select("filename", config.column)
            .groupBy("filename")
            .agg(struct(min(col(config.column)).alias("min"), max(col(config.column)).alias("max")).alias(rangeCol))
        accumDf.join(perFile, Seq("filename"), "full_outer")
      }
    }
  }

  /**
   * Writes the values of every large `(file, column)` pair into its per-column Delta table.
   *
   * @param sourceDf
   *   the base DataFrame with `filename` and all source columns (computed indexes already applied)
   * @param large
   *   the classification identifying which files are large for which columns
   */
  protected def handleLargeIndexes(sourceDf: DataFrame, large: LargeFileClassification): Unit = {
    // Columns with an existing table are included even when nothing is large this time, so a file
    // that shrank below the limit has its stale rows cleaned up.
    val columns = large.columns ++ largeIndexColumns.intersect(storageColumns)
    if (columns.nonEmpty) {
      logger.warn(s"Separating ${columns.size} column(s) into large index storage (limit=$largeIndexLimit)")
      appendToLargeIndex(sourceDf, large, columns)
    }
  }

  /**
   * Appends the processed DataFrame to the staging Delta table.
   *
   * Columns belonging to a large `(file, column)` pair are already `null` at this point: the index builders skip them
   * and their values were streamed to `large_indexes/` instead. Staging deliberately does '''not''' re-derive largeness
   * from array size. Doing so would be a second, independent decision, and any disagreement with [[classifyLargeFiles]]
   * would null out an array whose values were never written anywhere else, silently losing them.
   *
   * @param df
   *   the combined index DataFrame to stage
   */
  protected def appendToStaging(df: DataFrame): Unit = {
    val startTime = System.currentTimeMillis()

    val stagedDf =
      df
        .withColumn(StagingTimestampColumn, current_timestamp())
        .withColumn(StagingBatchIdColumn, lit(UUID.randomUUID().toString))
    val rowCount = stagedDf.count()
    logger.warn(s"Appending $rowCount rows to staging at $stagingFilePath")

    stagedDf.write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(stagingFilePath.toString)
    logger.warn(s"Staging append completed for '$name' ($rowCount rows) in ${System.currentTimeMillis() - startTime}ms")
  }

  /**
   * Appends large index data to per-column Delta tables under `large_indexes/`.
   *
   * For each column with at least one large file, the values of those files are read straight from the source DataFrame
   * as distinct `(filename, value)` rows and written to `large_indexes/{column}/`. Values are never collected into a
   * per-file array first, so a file's cardinality is bounded by what Delta can store rather than by what fits in an
   * executor-side array. Before appending, any existing rows for the same filenames are removed via Delta MERGE to
   * prevent duplicates (important during column backfill or re-indexing).
   *
   * @note
   *   The `count()` call before the write is intentional: it materializes the DataFrame to determine whether any
   *   large-index rows exist for this column, avoiding the overhead of a Delta MERGE + write when no rows exist. This
   *   results in a double computation (count + write), but the alternative—writing unconditionally—would create empty
   *   Delta commits and unnecessary MERGE operations.
   *
   * @param sourceDf
   *   the base DataFrame with `filename` and all source columns (computed indexes already applied)
   * @param large
   *   the classification identifying which files are large for which columns
   * @param columns
   *   the columns to process: those with large files in this batch, plus any with an existing table that may hold stale
   *   rows for the files being re-indexed
   */
  protected def appendToLargeIndex(sourceDf: DataFrame, large: LargeFileClassification, columns: Set[String]): Unit = {
    val startTime = System.currentTimeMillis()
    if (columns.nonEmpty) {
      // Dedup covers every file in the batch, not just the large ones: a file that was large on a
      // previous run but is not this time must still have its stale rows removed.
      val processedFiles = sourceDf.select("filename").distinct()

      columns.foreach { colName =>
        val columnPath = new Path(largeIndexesFilePath, colName)

        // Remove existing rows for these files to prevent duplicates during re-indexing.
        delta(columnPath) match {
          case Some(deltaTable) =>
            deltaTable
              .as("target")
              .merge(processedFiles.as("source"), "target.filename = source.filename")
              .whenMatched()
              .delete()
              .execute()
          case None => // No existing table, nothing to dedup
        }

        if (large.filesFor(colName).nonEmpty) {
          columnValueRows(sourceDf, colName).foreach { valueRows =>
            val columnData =
              valueRows
                .where(large.isLarge(colName))
                .where(col(colName).isNotNull)
                .distinct()

            val count = columnData.count()
            if (count > 0) {
              logger.warn(s"Appending $count rows to large index for column '$colName' at $columnPath")

              columnData.write
                .format("delta")
                .option("mergeSchema", "true")
                .mode("append")
                .save(columnPath.toString)
            }
          }
        }
      }
      logger.warn(s"Large index append completed for '$name' (${columns.size} columns) in ${System
          .currentTimeMillis() - startTime}ms")
    }
  }

  /**
   * Column name prefix for auto-bloom filter storage in the main index table.
   */
  protected val autoBloomColumnPrefix = "auto_bloom_"

  /**
   * Returns the set of auto-bloom storage column names (each prefixed with `auto_bloom_`).
   *
   * @return
   *   set of prefixed column names (e.g., `auto_bloom_user_id`)
   */
  protected def autoBloomStorageColumns: Set[String] =
    metadata.auto_bloom_indexes.asScala
      .map(c => autoBloomColumnPrefix + c)
      .toSet

  /**
   * Returns the set of columns eligible for auto-bloom filtering.
   *
   * Eligible columns are regular, computed, exploded-field, and temporal indexes. Temporal columns store
   * `(value, max_ts)` structs rather than scalars, so their filters are built over the `value` field alone — see
   * [[autoBloomValueColumn]]. Range indexes are ineligible: they store only per-file min/max bounds, which the range
   * predicate already prunes on.
   *
   * @return
   *   set of eligible column names
   */
  private def autoBloomEligibleColumns: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.as_column).toSet ++
      metadata.temporal_indexes.asScala.map(_.column).toSet

  /**
   * Builds auto-bloom filters for columns that have at least one large file.
   *
   * A column becomes auto-bloom the first time any file contributes at least `largeIndexLimit` distinct values to it.
   * From then on every file gets a filter, stored in the main index under the `auto_bloom_` prefix, so queries can
   * cheaply skip files before touching `large_indexes/`.
   *
   * Filters are folded directly from the source `(filename, value)` rows with a streaming aggregator rather than from a
   * collected array. Large files never have an array to read from, and building from the rows means a column's
   * cardinality is limited only by what the filter itself costs (~1.2 bytes per distinct value at 1% FPR).
   *
   * @param combinedDf
   *   the combined index DataFrame, keyed by `filename`
   * @param sourceDf
   *   the base DataFrame with `filename` and all source columns (computed indexes already applied)
   * @param large
   *   the classification identifying which files are large for which columns
   * @return
   *   `combinedDf` with an `auto_bloom_{column}` binary column per auto-bloom column; unchanged if none qualify
   */
  protected def buildAutoBloomIndexes(
      combinedDf: DataFrame,
      sourceDf: DataFrame,
      large: LargeFileClassification): DataFrame = {
    val startTime = System.currentTimeMillis()
    val eligible = autoBloomEligibleColumns
    if (eligible.isEmpty) combinedDf
    else {
      val columnsExceedingLimit = eligible.intersect(large.columns)
      logger.warn(s"Auto-bloom: checked ${eligible.size} columns, ${columnsExceedingLimit.size} exceed limit")

      columnsExceedingLimit.foreach { colName =>
        if (!metadata.auto_bloom_indexes.contains(colName)) {
          metadata.auto_bloom_indexes.add(colName)
        }
      }

      val autoBloomColumns = metadata.auto_bloom_indexes.asScala.toSet.intersect(eligible)

      if (autoBloomColumns.isEmpty) {
        logger.warn(s"Auto-bloom: no columns to build, completed in ${System.currentTimeMillis() - startTime}ms")
        combinedDf
      } else {
        logger.warn(s"Building auto-bloom filters for columns: ${autoBloomColumns.mkString(", ")}")
        val fpr = autoBloomFpr
        val result =
          autoBloomColumns.foldLeft(combinedDf) { (df, colName) =>
            columnValueRows(sourceDf, colName) match {
              case Some(valueRows) =>
                logger.warn(s"Auto-bloom: building filter for '$colName' with FPR=$fpr")
                val bloomColumn = autoBloomColumnPrefix + colName
                val bloomData =
                  buildStreamingBloomColumn(valueRows, autoBloomValueColumn(colName), bloomColumn, fpr)
                df.join(bloomData, Seq("filename"), "left")
              case None =>
                logger.warn(s"Auto-bloom: skipping '$colName', no value source available")
                df
            }
          }
        logger.warn(s"Auto-bloom: build completed in ${System.currentTimeMillis() - startTime}ms")
        result
      }
    }
  }

  /**
   * Compacts all Delta tables (main index and large index tables) using Delta OPTIMIZE.
   *
   * Runs Delta Lake's OPTIMIZE command to consolidate small files into larger ones, improving read performance.
   * Processes the main index table first, then each large index column table.
   */
  protected def compactDeltaTables(): Unit = {
    val overallStart = System.currentTimeMillis()
    val largeIdxCols = largeIndexColumns

    delta(indexFilePath).foreach { dt =>
      val compactStart = System.currentTimeMillis()
      logger.warn(s"Compacting main index at $indexFilePath")
      dt.optimize().executeCompaction()
      logger.warn(s"Compacted main index in ${System.currentTimeMillis() - compactStart}ms")
    }

    largeIdxCols.foreach { colName =>
      val columnPath = new Path(largeIndexesFilePath, colName)
      delta(columnPath).foreach { dt =>
        val compactStart = System.currentTimeMillis()
        logger.warn(s"Compacting large index for column $colName at $columnPath")
        dt.optimize().executeCompaction()
        logger.warn(s"Compacted large index '$colName' in ${System.currentTimeMillis() - compactStart}ms")
      }
    }

    logger.warn(s"Compaction completed for main index and ${largeIdxCols.size} large index table(s) in ${System
        .currentTimeMillis() - overallStart}ms")
  }

  /**
   * Counter tracking batches processed since the last auto-compaction.
   *
   * Incremented after each batch in `updateBatched` and reset to zero after each compaction cycle or at the start of
   * each [[Index.update update]] call to prevent stale counts from a previous `update` from triggering premature
   * compaction.
   */
  protected var batchesSinceCompact: Int = 0

  /**
   * Triggers compaction if the auto-compact threshold has been reached.
   *
   * Checks the [[batchesSinceCompact]] counter against the configured `autoCompactThreshold`. If the threshold is met,
   * compacts all Delta tables and resets the counter to zero. The counter is persisted to metadata so it survives
   * across Spark jobs.
   */
  protected def maybeAutoCompact(): Unit =
    autoCompactThreshold.foreach { threshold =>
      if (batchesSinceCompact >= threshold) {
        logger.warn(s"Auto-compact threshold reached ($batchesSinceCompact batches), compacting Delta tables")
        compactDeltaTables()
        batchesSinceCompact = 0
        metadata.batches_since_compact = 0
        writeMetadata(metadata)
      }
    }

  /**
   * Vacuums all Delta tables (main index and large index tables) to remove old files.
   *
   * Uses Delta Lake's VACUUM command to delete data files no longer referenced by the transaction log. Temporarily
   * disables the retention duration safety check when `retentionHours` is zero or negative.
   *
   * @note
   *   '''Thread-safety:''' This method temporarily mutates the shared SparkConf (`retentionDurationCheck.enabled`).
   *   Concurrent `Index` instances sharing the same `SparkSession` may race on this setting. The value is restored in a
   *   `finally` block, but a TOCTOU window exists between set and restore.
   *
   * @param retentionHours
   *   number of hours of history to retain (default 168 = 7 days)
   */
  protected def vacuumDeltaTables(retentionHours: Int = 168): Unit = {
    val overallStart = System.currentTimeMillis()
    val largeIdxCols = largeIndexColumns
    val previousCheck = spark.conf.getOption("spark.databricks.delta.retentionDurationCheck.enabled")
    try {
      if (retentionHours <= 0) {
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      }
      delta(indexFilePath).foreach { dt =>
        logger.warn(s"Vacuuming main index at $indexFilePath with retention $retentionHours hours")
        dt.vacuum(retentionHours.toDouble)
      }

      largeIdxCols.foreach { colName =>
        val columnPath = new Path(largeIndexesFilePath, colName)
        delta(columnPath).foreach { dt =>
          logger.warn(s"Vacuuming large index for column $colName at $columnPath with retention $retentionHours hours")
          dt.vacuum(retentionHours.toDouble)
        }
      }

      logger.warn(s"Vacuum completed for main index and ${largeIdxCols.size} large index table(s) in ${System
          .currentTimeMillis() - overallStart}ms")
    } finally {
      previousCheck match {
        case Some(v) =>
          spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", v)
        case None =>
          spark.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")
      }
    }
  }

  /**
   * Consolidates staged data into the main index table.
   *
   * Delegates to [[consolidateMainStaging]] which performs a Delta MERGE (upsert) of all staged rows into the main
   * index, then deletes the staging table. Logs the total time taken.
   */
  protected def consolidateStaging(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.warn("Starting consolidation of staged data")
    consolidateMainStaging()

    logger.warn(s"Consolidation complete in ${System.currentTimeMillis() - startTime}ms")
  }

  /**
   * Consolidates the main staging table into the main index via Delta MERGE.
   *
   * Performs an upsert (match on `filename`): existing rows are updated, new rows are inserted. Creates the main index
   * if it does not yet exist by writing the staging data directly. Schema auto-merge is temporarily enabled to support
   * new columns added since the index was first created.
   *
   * After successful merge, the staging Delta table is deleted.
   */
  private def consolidateMainStaging(): Unit =
    if (!exists(stagingFilePath)) {
      logger.warn("No staging data to consolidate for main index")
    } else {

      val allStorageColumns =
        storageColumns ++ bloomStorageColumns ++ rangeStorageColumns ++ autoBloomStorageColumns
      val rawStagingDf =
        spark.read
          .format("delta")
          .load(stagingFilePath.toString)
      val stagingDf = selectStagingRows(rawStagingDf)
      val stagingRowCount = stagingDf.count()
      logger.warn(s"Merging $stagingRowCount staged rows into main index")

      if (allStorageColumns.nonEmpty) {
        delta(indexFilePath) match {
          case Some(deltaTable) =>
            logger.warn(s"Merging staging data into main index at $indexFilePath")
            // Delta 3.2+: per-merge schema evolution; no shared session config.
            deltaTable
              .as("target")
              .merge(stagingDf.as("source"), "target.filename = source.filename")
              .withSchemaEvolution()
              .whenMatched()
              .updateAll()
              .whenNotMatched()
              .insertAll()
              .execute()
          case None =>
            logger.warn(s"Creating new main index from staging at $indexFilePath")
            stagingDf.write
              .format("delta")
              .mode("overwrite")
              .save(indexFilePath.toString)
        }
      } else {
        stagingDf.write
          .format("delta")
          .mode("overwrite")
          .save(indexFilePath.toString)
      }

      // Delete staging after successful consolidation
      try {
        delete(stagingFilePath)
        logger.warn(s"Deleted main staging table after consolidation for index '$name'")
      } catch {
        case e: Exception =>
          logger.warn(
            s"Failed to delete staging table after consolidation for index '$name': ${e.getMessage}. " +
              "Staging data may be re-merged on next consolidation (idempotent).",
            e)
      }
    }

  private def selectStagingRows(stagingDf: DataFrame): DataFrame = {
    val payloadColumns =
      stagingDf.columns.filterNot(column => column == StagingTimestampColumn || column == StagingBatchIdColumn)
    val completenessExpressions =
      payloadColumns.filterNot(_ == "filename").map(column => when(col(column).isNotNull, 1).otherwise(0))
    val completeness =
      completenessExpressions.reduceOption(_ + _).getOrElse(lit(0))
    val canonicalHash =
      sha2(to_json(struct(payloadColumns.sorted.map(col): _*)), 256)
    val timestampOrder =
      if (stagingDf.columns.contains(StagingTimestampColumn)) {
        col(StagingTimestampColumn).desc_nulls_last
      } else {
        lit(0).desc
      }
    val batchOrder =
      if (stagingDf.columns.contains(StagingBatchIdColumn)) {
        col(StagingBatchIdColumn).desc_nulls_last
      } else {
        lit("").desc
      }
    val rankWindow =
      Window
        .partitionBy("filename")
        .orderBy(timestampOrder, col(StagingCompletenessColumn).desc, batchOrder, col(StagingHashColumn).desc)

    stagingDf
      .withColumn(StagingCompletenessColumn, completeness)
      .withColumn(StagingHashColumn, canonicalHash)
      .withColumn(StagingRankColumn, row_number().over(rankWindow))
      .where(col(StagingRankColumn) === 1)
      .drop(
        StagingTimestampColumn,
        StagingBatchIdColumn,
        StagingRankColumn,
        StagingCompletenessColumn,
        StagingHashColumn)
  }

}
