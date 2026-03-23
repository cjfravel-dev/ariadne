# Ariadne

Like [Ariadne](https://en.wikipedia.org/wiki/Ariadne) from Greek mythology, this library helps you navigate your data labyrinth. And just like Ariadne, I hope you'll one day betray it—like Theseus—and move on to something better, such as [Apache Iceberg](https://iceberg.apache.org/) or [Delta Lake](https://delta.io).

But in the meantime, if your data lake is more of a data swamp and you lack a way to generate simple indexes for your files, Ariadne can help.

## Overview

Ariadne enables you to create simple indexes, allowing you to efficiently locate the files needed for your joins. To persist indexes across runs, set the configuration value `spark.ariadne.storagePath`, making sure it is accessible via `spark.sparkContext.hadoopConfiguration`.

### Supported Formats

Ariadne supports three data formats:

- **Parquet** - Columnar format (default)
- **CSV** - Comma-separated values
- **JSON** - JavaScript Object Notation

Additional format-specific options can be provided via `readOptions` when creating an index.

### How It Works

1. **Define an index** – Provide a name, schema, file format, and optional read options.
2. **Specify indexed columns** – Choose the columns you want to index:
   - **Regular indexes** – Index standard columns directly (`addIndex("column_name")`)
   - **Bloom filter indexes** – Space-efficient probabilistic indexes for high-cardinality columns (`addBloomIndex("column_name", fpr)`)
   - **Temporal indexes** – Version-aware indexes that deduplicate by recency (`addTemporalIndex("column_name", "timestamp_column")`)
   - **Computed indexes** – Index derived values using SQL expressions (`addComputedIndex("alias", "expression")`)
   - **Exploded field indexes** – Index elements within array columns (`addExplodedFieldIndex("array_column", "field_path", "alias")`)
3. **Add files** – Register files with the index.
4. **Update the index** – Run updates whenever you add new files or indexed columns.
5. **Use the index in joins** – Leverage the index to load only the relevant files based on your DataFrame's join conditions.

> **Multi-column joins**: When joining on multiple columns, Ariadne uses AND semantics — a file must match on *all* queried index types to be included. This applies across regular, bloom, temporal, and range indexes.

> **Note:** When using an index in a join, it is no longer a "narrow" transformation. The index must first retrieve matching values from its records, load the appropriate data, and then perform the narrow transformation on the resulting DataFrame.

### Example Usage

```xml
<!-- Spark 3.4 / Delta 2.4 (Azure Synapse) -->
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne-spark34_2.12</artifactId>
    <version>0.0.1-alpha-44</version>
</dependency>

<!-- Spark 3.5 / Delta 3.2 -->
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne-spark35_2.12</artifactId>
    <version>0.0.1-alpha-44</version>
</dependency>
```

```scala
val files = // Array(...)

val table = spark.read.schema(tableSchema).parquet(files: _*)
val otherDf = // spark.read ....

val joinedWithoutIndex = otherDf.join(table, Seq("version", "id"), "left_semi")

// Set the storage path for Ariadne indexes
// ensure spark.sparkContext.hadoopConfiguration has access to this location
spark.conf.set("spark.ariadne.storagePath", s"abfss://${container}@${storageAccount}.dfs.core.windows.net/ariadne")

import dev.cjfravel.ariadne

// Create and configure the index
val index = Index("table", tableSchema, "parquet")
index.addIndex("version")
index.addFile(files: _*)
index.update

// Use the index in the join
val dfJoinedAgainstIndex = otherDf.join(index, Seq("version", "id"), "left_semi") // records in otherDf that have matching data in indexed files
val indexJoinedAgainstDf = index.join(otherDf, Seq("version", "id"), "left_semi") // matching data that has been indexed that is in otherDf

// you can add computed indexes using standard Column types
index.addComputedIndex("category", "substring(Id, 1, 4)")
index.update

// you can add exploded field indexes for nested data
index.addExplodedFieldIndex("users", "id", "user_id")    // index users[].id as "user_id"
index.addExplodedFieldIndex("tags", "name", "tag_name")  // index tags[].name as "tag_name"
index.update

val userQueryDf = // spark.read ....
val joinedOnExplodedField = userQueryDf.join(index, Seq("user_id"), "left_semi")
```

### JSON Format Example

```scala
// JSON with read options (e.g., for multi-line JSON arrays)
val readOptions = Map("multiLine" -> "true")
val jsonIndex = Index("events", jsonSchema, "json", readOptions)
jsonIndex.addExplodedFieldIndex("users", "id", "user_id")  // index users[].id as "user_id"
jsonIndex.addFile("events.json")
jsonIndex.update

val queryDf = // spark.read ....
val result = queryDf.join(jsonIndex, Seq("user_id"), "left_semi")
```

### Bloom Filter Index Example

Bloom filters are ideal for high-cardinality columns (like user IDs) where storing all distinct values would be expensive:

```scala
// Create an index with bloom filter for high-cardinality ID columns
val index = Index("events", eventSchema, "parquet")
index.addBloomIndex("user_id", fpr = 0.01)  // 1% false positive rate
index.addBloomIndex("session_id", fpr = 0.001)  // 0.1% FPR for more accuracy
index.addFile(eventFiles: _*)
index.update

// Query works the same as regular indexes
val userQueryDf = // spark.read ....
val result = index.join(userQueryDf, Seq("user_id"), "inner")
```

**Key points about bloom filter indexes:**

- **Space efficient**: ~10 bits per element at 1% FPR vs. storing actual values
- **Probabilistic**: May return files that don't contain the value (false positives), but never misses files that do contain it (no false negatives)
- **Mutually exclusive**: A column can have either a regular index OR a bloom index, not both
- **Best for**: High-cardinality columns across large datasets where exact value storage would be prohibitive

**Automatic bloom filters for large indexes**: When a column exceeds `largeIndexLimit` distinct values, Ariadne automatically creates a bloom filter for that column in addition to the large index table. This pre-filters candidate files before querying the expensive large index, significantly reducing I/O. The false positive rate is controlled by `spark.ariadne.autoBloomFpr` (default: 1%). This feature is backward-compatible — existing indexes are automatically migrated on the next `update`.

### Temporal Index Example

Temporal indexes are ideal when the same entity appears in multiple files at different timestamps, and you only want the most recent version:

```scala
// Create an index with temporal deduplication
val index = Index("users", userSchema, "parquet")
index.addTemporalIndex("user_id", "updated_at")  // Dedup by user_id, keep latest updated_at
index.addFile(userFiles: _*)
index.update

// Join returns only the latest version of each user_id
val queryDf = // spark.read ....
val result = index.join(queryDf, Seq("user_id"), "inner")
// If user_id=1 appears in file_jan.parquet (updated_at=2024-01) and
// file_jun.parquet (updated_at=2024-06), only the June version is returned.
```

**Key points about temporal indexes:**

- **Latest-version semantics**: When the same value exists in multiple files, only the row with the most recent timestamp is returned during joins
- **Null timestamps**: Rows with null timestamps are ranked last — a non-null timestamp always wins
- **Mutually exclusive**: A column can have either a regular, bloom, computed, OR temporal index — not multiple
- **Best for**: Slowly changing dimensions, entity snapshots, event-sourced data where you need the current state

### Range Index Example

Range indexes store per-file min/max values for a column, enabling efficient file pruning at query time. Files are only loaded when the query values fall within their min/max range.

```scala
// Create an index with range-based pruning for date or numeric columns
val index = Index("events", eventSchema, "parquet")
index.addRangeIndex("event_date")   // Store min/max event_date per file
index.addRangeIndex("amount")       // Store min/max amount per file
index.addIndex("user_id")           // Regular index alongside range indexes
index.addFile(eventFiles: _*)
index.update

// During joins, range indexes automatically prune files that can't contain matching values
val queryDf = // DataFrame with event_date and user_id columns
val result = index.join(queryDf, Seq("event_date", "user_id"), "inner")
// Only files where event_date range overlaps with query values are loaded
```

**Key points about range indexes:**

- **Min/max pruning**: Each file stores the minimum and maximum value for the indexed column. At query time, files are skipped if no query value falls within their range
- **Best for ordered data**: Most effective when data has natural ordering (dates, timestamps, sequential IDs, amounts)
- **Complementary**: Range indexes work alongside regular, bloom, and temporal indexes — results are intersected across all index types
- **Supported types**: Works with any comparable Spark type (DateType, TimestampType, IntegerType, LongType, DoubleType, StringType, etc.)
- **Mutually exclusive**: A column can have only one index type

## Index Maintenance

### Deleting Files

Remove files from the index when they are no longer needed (e.g., after data archival or deletion):

```scala
// Remove specific files from the index
index.deleteFiles("old_data_2023.parquet", "archived_events.parquet")

// Removes entries from: main index, large index tables, staging, and file list
```

### Compaction

Delta Lake tables accumulate small files over time from repeated updates. Compaction consolidates these into larger files for better read performance:

```scala
// Manually compact all index Delta tables
index.compact()

// Vacuum old Delta log files (default: 168 hours / 7 days retention)
index.vacuum()
index.vacuum(retentionHours = 72)  // Custom retention period
```

**Auto-compaction**: Set `spark.ariadne.autoCompactThreshold` to automatically compact after a specified number of update batches:

```scala
spark.conf.set("spark.ariadne.autoCompactThreshold", "10")  // Compact every 10 batches
index.update  // Automatically compacts during update
```

## Configuration

All configuration is done via Spark configuration properties. Set them before creating or using indexes.

| Configuration Key                             | Type    | Default      | Description                                                                                                                                                                         |
| --------------------------------------------- | ------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.ariadne.storagePath`                   | String  | _(required)_ | Path on the filesystem where Ariadne stores index data. Must be accessible via `spark.sparkContext.hadoopConfiguration`.                                                            |
| `spark.ariadne.largeIndexLimit`               | Long    | `500000`     | Maximum number of distinct values per column per file before an index is considered "large" and stored in a separate consolidated Delta table.                                      |
| `spark.ariadne.stagingConsolidationThreshold` | Int     | `50`         | Number of batches to process before consolidating staged data into the main index during `update`. Provides fault tolerance for large index builds.                                 |
| `spark.ariadne.indexRepartitionCount`         | Int     | _(not set)_  | Number of partitions to repartition the index metadata DataFrame to during file lookup. Helps avoid `FetchFailedException` when exploding large index arrays. |
| `spark.ariadne.repartitionDataFiles`          | Boolean | `false`      | When `true`, also applies `indexRepartitionCount` repartitioning to data files read during joins. When `false` (default), data files keep their natural parquet partitioning. |
| `spark.ariadne.debug`                         | Boolean | `false`      | Enables detailed diagnostics during join operations: per-phase timing, file sizes, physical plans, and cache materialization stats. |
| `spark.ariadne.lockTimeout`                   | Long    | `1800`       | Seconds after last lock refresh before a lock is considered stale and eligible for auto-healing. Default is 30 minutes. |
| `spark.ariadne.lockRetryInterval`             | Long    | `60`         | Base interval in seconds between lock acquisition retries. Exponential backoff is applied up to a 60-second cap per retry. |
| `spark.ariadne.lockMaxWait`                   | Long    | `3600`       | Maximum total seconds to wait for lock acquisition before throwing `IndexLockException`. Default is 1 hour. |
| `spark.ariadne.lockRefreshInterval`           | Int     | `1`          | Refresh the update lock every N batches during `update`. Keeps the lock from appearing stale during long-running updates. |
| `spark.ariadne.autoCompactThreshold`          | Int     | _(not set)_  | Number of update batches before triggering automatic Delta table compaction. When not set, auto-compaction is disabled. |
| `spark.ariadne.autoBloomFpr`                  | Double  | `0.01`       | False positive rate for automatic bloom filters on large index columns. Only applies to columns that exceed `largeIndexLimit`. |

### Example

```scala
// Required: storage path
spark.conf.set("spark.ariadne.storagePath", "abfss://container@account.dfs.core.windows.net/ariadne")

// Optional: tune for large indexes
spark.conf.set("spark.ariadne.largeIndexLimit", "1000000")
spark.conf.set("spark.ariadne.stagingConsolidationThreshold", "100")

// Optional: prevent FetchFailedException on very large index metadata
spark.conf.set("spark.ariadne.indexRepartitionCount", "500")

// Optional: also repartition data files (disabled by default)
spark.conf.set("spark.ariadne.repartitionDataFiles", "true")

// Optional: enable debug logging
spark.conf.set("spark.ariadne.debug", "true")

// Optional: auto-compact index after every 10 update batches
spark.conf.set("spark.ariadne.autoCompactThreshold", "10")

// Optional: tune auto-bloom false positive rate for large indexes (default: 1%)
spark.conf.set("spark.ariadne.autoBloomFpr", "0.005")

// Optional: tune lock behavior for concurrent jobs
spark.conf.set("spark.ariadne.lockTimeout", "1800")        // 30 min stale threshold
spark.conf.set("spark.ariadne.lockRetryInterval", "60")     // 1 min base retry interval
spark.conf.set("spark.ariadne.lockMaxWait", "3600")         // 1 hr max wait
```

### Concurrency & Locking

Ariadne uses per-index file-based locks to prevent concurrent updates from corrupting index data. Locks are automatically acquired and released during `addFile()` and `update()` operations.

**How it works:**

- **Two separate locks per index:** `.filelist.lock` (for `addFile`) and `.update.lock` (for `update`)
- **Automatic refresh:** During `update`, the lock is refreshed every N batches (configurable via `spark.ariadne.lockRefreshInterval`) to signal the job is still alive
- **Wait and retry:** If a lock is held by another job, the caller retries with exponential backoff up to `spark.ariadne.lockMaxWait` seconds
- **Auto-healing:** If a lock's `lastRefreshedAt` timestamp is older than `spark.ariadne.lockTimeout`, it is considered stale (e.g., the holding job crashed) and is automatically broken so the new job can proceed

**Lock file format:** JSON files stored at `{indexStoragePath}/.filelist.lock` and `{indexStoragePath}/.update.lock`, containing a correlation ID, timestamps, and the Spark application ID of the lock holder for diagnostics.
