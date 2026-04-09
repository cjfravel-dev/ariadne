# Ariadne

Like [Ariadne](https://en.wikipedia.org/wiki/Ariadne) from Greek mythology, this library helps you navigate your data labyrinth. And just like Ariadne, I hope you'll one day betray it—like Theseus—and move on to something better, such as [Apache Iceberg](https://iceberg.apache.org/) or [Delta Lake](https://delta.io).

But in the meantime, if your data lake is more of a data swamp and you lack a way to generate simple indexes for your files, Ariadne can help.

## What Does Ariadne Do?

If you're joining a DataFrame against a large collection of files, Spark normally has to read *all* of them. Ariadne builds lightweight indexes over those files so that only the relevant ones are loaded, skipping files that can't contain matching rows.

It works with **Parquet**, **CSV**, and **JSON** files, stores its indexes as Delta Lake tables, and integrates directly into your existing Spark joins with minimal code changes.

## Quick Start

### Installation

```xml
<!-- Spark 3.5 / Delta 3.2 (default) -->
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne-spark35_2.12</artifactId>
    <version>0.1.1-beta</version>
</dependency>

<!-- Spark 3.4 / Delta 2.4 (Azure Synapse) -->
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne-spark34_2.12</artifactId>
    <version>0.1.1-beta</version>
</dependency>
```

### Basic Usage

```scala
import dev.cjfravel.ariadne.Index
import dev.cjfravel.ariadne.Index._  // for df.join(index, ...) implicit

// Tell Ariadne where to store indexes (any Hadoop-accessible path)
spark.conf.set("spark.ariadne.storagePath", s"abfss://${container}@${storageAccount}.dfs.core.windows.net/ariadne")

// 1. Create an index
val index = Index("orders", orderSchema, "parquet")

// 2. Tell it which columns to index and which files to track
index.addIndex("customer_id")
index.addFile(orderFiles: _*)

// 3. Build the index
index.update

// 4. Use it in a join — Ariadne loads only the files that match
val result = customerDf.join(index, Seq("customer_id"), "inner")
```

That's it. The join works exactly like a normal Spark join, but under the hood Ariadne skips files that can't possibly contain matching rows.

You can also join in the other direction:

```scala
val result = index.join(customerDf, Seq("customer_id"), "inner")
```

### Reconnecting to an Existing Index

Once an index has been created, you can reconnect to it without repeating the schema or format:

```scala
val index = Index("orders")
```

## Choosing an Index Type

Ariadne offers several index types, each suited to different data patterns. A column can have exactly one index type.

### Regular Index

The simplest option. Stores the distinct values found in each file. Great for low-to-medium cardinality columns.

```scala
index.addIndex("region")
```

### Bloom Filter Index

A space-efficient alternative for high-cardinality columns (like user IDs) where storing every distinct value would be too expensive. Bloom filters may occasionally include a file that doesn't actually match (false positive), but will never miss a file that does.

```scala
index.addBloomIndex("user_id", fpr = 0.01)    // 1% false positive rate
index.addBloomIndex("session_id", fpr = 0.001) // 0.1% for higher accuracy
```

> **Auto-bloom for large columns**: When a regular index column exceeds the `largeIndexLimit` threshold, Ariadne automatically adds a bloom filter to speed up lookups. The false positive rate is controlled by `spark.ariadne.autoBloomFpr` (default: 1%).

### Temporal Index

Use this when the same entity appears in multiple files over time and you only want the most recent version. During joins, Ariadne automatically deduplicates — keeping only the row with the latest timestamp.

```scala
index.addTemporalIndex("user_id", "updated_at")

// If user_id=1 exists in both january.parquet and june.parquet,
// only the June row is returned.
```

### Range Index

Stores per-file min/max values. At query time, files are skipped entirely if no query value falls within their range. Most useful for naturally ordered data like dates or amounts.

```scala
index.addRangeIndex("event_date")
index.addRangeIndex("amount")
```

### Computed Index

Index a derived value using a SQL expression, without needing it to exist as a column in your files.

```scala
index.addComputedIndex("category", "substring(Id, 1, 4)")
```

### Exploded Field Index

Index elements inside array columns — useful for nested data.

```scala
index.addExplodedFieldIndex("users", "id", "user_id")    // index users[].id as "user_id"
index.addExplodedFieldIndex("tags", "name", "tag_name")   // index tags[].name as "tag_name"
```

### Combining Index Types

Different index types work together. When you join on multiple columns, Ariadne uses AND semantics — a file must match on *all* indexed columns to be included.

```scala
val index = Index("events", eventSchema, "parquet")
index.addRangeIndex("event_date")     // prune by date range
index.addBloomIndex("user_id", 0.01)  // filter by user ID
index.addIndex("region")              // filter by region
index.addFile(eventFiles: _*)
index.update

val result = index.join(queryDf, Seq("event_date", "user_id", "region"), "inner")
```

## Working with Indexes

### Column Selection

Reduce I/O by reading only the columns you need:

```scala
val result = index.select("user_id", "name", "email").join(queryDf, Seq("user_id"), "inner")
```

### File Lookup Without Joining

Find which files contain specific values without performing a full join:

```scala
val files: Set[String] = index.locateFiles(Map(
  "user_id" -> Array("u1", "u2", "u3")
))
```

### JSON Format

Pass format-specific read options when creating the index:

```scala
val jsonIndex = Index("events", jsonSchema, "json", readOptions = Map("multiLine" -> "true"))
jsonIndex.addExplodedFieldIndex("users", "id", "user_id")
jsonIndex.addFile("events.json")
jsonIndex.update
```

### Schema Evolution

If your data schema changes over time, reconnect with the new schema using `allowSchemaMismatch`:

```scala
val index = Index("myIndex", newSchema, "parquet", allowSchemaMismatch = true)
```

All previously indexed columns must still exist in the new schema.

### Index Catalog

Use `IndexCatalog` to discover and manage all indexes under your storage path:

```scala
import dev.cjfravel.ariadne.IndexCatalog

IndexCatalog.list()                 // list all index names
IndexCatalog.exists("myIndex")      // check if an index exists
IndexCatalog.describe("myIndex")    // get a summary (index types, file count, format)
IndexCatalog.describeAll()          // get summaries for all indexes
IndexCatalog.toDF().show()          // view all indexes as a DataFrame
IndexCatalog.get("myIndex")         // reconnect to an existing index

// Find and clean up indexes that reference a deleted file
IndexCatalog.findIndexes("s3a://bucket/data/old_file.parquet").foreach { name =>
  IndexCatalog.get(name).deleteFiles("s3a://bucket/data/old_file.parquet")
}
```

### Spark SQL Catalog

Ariadne indexes can be queried through Spark SQL by registering the Ariadne catalog:

```scala
// spark.sql.extensions must be set on SparkConf BEFORE creating the SparkContext
val conf = new SparkConf()
  .set("spark.sql.catalog.ariadne", "dev.cjfravel.ariadne.catalog.AriadneCatalog")
  .set("spark.sql.extensions", "dev.cjfravel.ariadne.catalog.AriadneSparkExtension")
```

All indexes under `spark.ariadne.storagePath` are automatically discoverable — no per-index registration needed:

```sql
SHOW TABLES IN ariadne;
DESCRIBE ariadne.customers;
SELECT * FROM ariadne.customers WHERE id = 123;
SELECT * FROM ariadne.customers c JOIN orders o ON c.id = o.customerid;
```

**How optimization works:**

| Query Pattern | Optimization |
|--------------|-------------|
| `SELECT * FROM ariadne.x` | Full scan of all source data files |
| `SELECT * FROM ariadne.x WHERE col = val` | `locateFiles()` prunes files before reading |
| `SELECT * FROM ariadne.x JOIN y ON ...` | Catalyst rule pre-prunes Ariadne files via `locateFiles()` — only INNER equi-joins on same-name, fully-indexed columns are optimized; all others fall back to full scan |

The Ariadne catalog is read-only. Index creation, updates, and deletion are performed through the Scala API.

### Inspection

```scala
index.indexes                           // set of all indexed column names
index.hasFile("data/file1.parquet")     // check if a file is tracked
index.format                            // data file format (e.g., "parquet")
index.storedSchema                      // the schema stored in the index metadata
index.stats().show()                    // per-column statistics
index.refreshMetadata()                 // reload metadata from disk
Index.exists("myIndex")                 // check if an index exists
```

## Index Maintenance

### Removing Files

When files are archived or deleted, remove them from the index:

```scala
index.deleteFiles("old_data_2023.parquet", "archived_events.parquet")
```

### Compaction & Vacuum

Over time, Delta Lake tables accumulate small files from repeated updates. Compact them for better read performance:

```scala
index.compact()
index.vacuum()                          // default: 7-day retention
index.vacuum(retentionHours = 72)       // custom retention
```

You can also enable auto-compaction so this happens automatically during updates:

```scala
spark.conf.set("spark.ariadne.autoCompactThreshold", "10")  // compact every 10 batches
```

### Removing an Index

```scala
Index.remove("myIndex")          // delete the index and all its data
IndexCatalog.remove("myIndex")   // same, via the catalog
```

### Pruning Metrics

Ariadne automatically logs how effective each join's pruning was:

```
Index pruning: loaded 42 of 10000 files (1.23 GB of 45.67 GB) — 97% data pruned
```

## Configuration

All settings are Spark configuration properties. Only `storagePath` is required.

| Key | Default | Description |
|-----|---------|-------------|
| `spark.ariadne.storagePath` | _(required)_ | Where Ariadne stores index data. Must be Hadoop-accessible. |
| `spark.ariadne.largeIndexLimit` | `500000` | Distinct values per column before switching to a separate large-index table. |
| `spark.ariadne.stagingConsolidationThreshold` | `50` | Batches between staging consolidations during `update`. |
| `spark.ariadne.indexRepartitionCount` | _(not set)_ | Repartition index metadata to avoid `FetchFailedException` on large indexes. |
| `spark.ariadne.repartitionDataFiles` | `false` | Also repartition data files during joins (not just index metadata). |
| `spark.ariadne.autoCompactThreshold` | _(not set)_ | Auto-compact after this many update batches. |
| `spark.ariadne.autoBloomFpr` | `0.01` | False positive rate for auto-bloom filters on large columns. |
| `spark.ariadne.debug` | `false` | Log detailed join diagnostics (timing, file sizes, physical plans). |
| `spark.ariadne.lockTimeout` | `1800` | Seconds before a lock is considered stale (default: 30 min). |
| `spark.ariadne.lockRetryInterval` | `60` | Base retry interval for lock acquisition (with exponential backoff). |
| `spark.ariadne.lockMaxWait` | `3600` | Max seconds to wait for a lock before failing (default: 1 hr). |
| `spark.ariadne.lockRefreshInterval` | `1` | Refresh the lock every N batches during `update`. |
| `spark.sql.catalog.ariadne` | _(not set)_ | Set to `dev.cjfravel.ariadne.catalog.AriadneCatalog` to enable Spark SQL access. |
| `spark.sql.extensions` | _(not set)_ | Add `dev.cjfravel.ariadne.catalog.AriadneSparkExtension` for JOIN optimization. |

### Concurrency

Ariadne uses file-based locks to prevent concurrent modifications. Locks are acquired and released automatically. If a job crashes and leaves a stale lock, it will auto-heal after `lockTimeout` seconds.

Two locks are used per index: `.filelist.lock` (for `addFile`) and `.update.lock` (for `update`, `deleteFiles`, `compact`, and `vacuum`).

## Error Handling

All Ariadne exceptions extend `AriadneException`, so you can catch them with a single handler:

```scala
import dev.cjfravel.ariadne.exceptions._

try {
  index.update
} catch {
  case e: AriadneException => // handle any Ariadne error
}
```

| Exception | When It Happens |
|-----------|-----------------|
| `SchemaNotProvidedException` | Creating a new index without a schema |
| `SchemaMismatchException` | Schema changed and `allowSchemaMismatch` is `false` |
| `FormatMismatchException` | Format doesn't match what's stored |
| `IndexNotFoundException` | Removing an index that doesn't exist |
| `IndexNotFoundInNewSchemaException` | New schema is missing a previously indexed column |
| `ColumnNotFoundException` | Referencing a column that's not in the schema or indexes |
| `IndexLockException` | Lock acquisition timed out |
| `MetadataMissingOrCorruptException` | Index metadata is unreadable — delete and re-create the index |
| `MissingFormatException` | Creating a new index without a format |
| `MissingSchemaException` | Schema field is null in metadata |
| `SchemaParseException` | Stored schema string can't be parsed |
| `FileListNotFoundException` | Removing a file list that doesn't exist |

## Troubleshooting

### `FetchFailedException` During Joins

Large index arrays can overwhelm Spark's shuffle. Spread the work across more partitions:

```scala
spark.conf.set("spark.ariadne.indexRepartitionCount", "500")
spark.conf.set("spark.ariadne.repartitionDataFiles", "true")  // if data files are also large
```

### `IndexLockException`

Another job may hold the lock, or a crashed job left a stale one. Stale locks auto-heal after `lockTimeout` seconds (default: 30 min). If you're certain no other job is running, you can delete the lock file manually:

```
{storagePath}/{indexName}/.update.lock
{storagePath}/{indexName}/.filelist.lock
```

### Out of Memory During Updates

High-cardinality columns can cause large in-memory arrays. Try:

1. **Lower the large index threshold** to split high-cardinality columns into separate tables:
   ```scala
   spark.conf.set("spark.ariadne.largeIndexLimit", "100000")
   ```
2. **Use bloom indexes** instead of regular indexes for high-cardinality columns:
   ```scala
   index.addBloomIndex("high_cardinality_col", fpr = 0.01)
   ```

### `MetadataMissingOrCorruptException`

The index metadata file is damaged. Remove and re-create the index:

```scala
Index.remove("myIndex")
val index = Index("myIndex", schema, "parquet")
// re-add indexes and files, then update
```

### `SchemaMismatchException`

Your data schema changed. Reconnect with the new schema:

```scala
val index = Index("myIndex", newSchema, "parquet", allowSchemaMismatch = true)
```
