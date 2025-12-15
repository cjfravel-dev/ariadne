# Ariadne  

Like [Ariadne](https://en.wikipedia.org/wiki/Ariadne) from Greek mythology, this library helps you navigate your data labyrinth. And just like Ariadne, I hope you’ll one day betray it—like Theseus—and move on to something better, such as [Apache Iceberg](https://iceberg.apache.org/) or [Delta Lake](https://delta.io).  

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
   - **Computed indexes** – Index derived values using SQL expressions (`addComputedIndex("alias", "expression")`)
   - **Exploded field indexes** – Index elements within array columns (`addExplodedFieldIndex("array_column", "field_path", "alias")`)
3. **Add files** – Register files with the index.
4. **Update the index** – Run updates whenever you add new files or indexed columns.
5. **Use the index in joins** – Leverage the index to load only the relevant files based on your DataFrame's join conditions.

> **Note:** When using an index in a join, it is no longer a "narrow" transformation. The index must first retrieve matching values from its records, load the appropriate data, and then perform the narrow transformation on the resulting DataFrame.

### Example Usage  
```xml
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne</artifactId>
    <version>0.0.1-alpha-32</version>
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