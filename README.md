# Ariadne  

Like [Ariadne](https://en.wikipedia.org/wiki/Ariadne) from Greek mythology, this library helps you navigate your data labyrinth. And just like Ariadne, I hope you’ll one day betray it—like Theseus—and move on to something better, such as [Apache Iceberg](https://iceberg.apache.org/) or [Delta Lake](https://delta.io).  

But in the meantime, if your data lake is more of a data swamp and you lack a way to generate simple indexes for your files, Ariadne can help.  

## Overview  

Ariadne enables you to create simple indexes, allowing you to efficiently locate the files needed for your joins. To persist indexes across runs, set the configuration value `spark.ariadne.storagePath`, making sure it is accessible via `spark.sparkContext.hadoopConfiguration`.  

### How It Works  

1. **Define an index** – Provide a name, schema, and file format.  
2. **Specify indexed columns** – Choose the columns you want to index.  
3. **Add files** – Register files with the index.  
4. **Update the index** – Run updates whenever you add new files or indexed columns.  
5. **Use the index in joins** – Leverage the index to load only the relevant files based on your DataFrame’s join conditions.  

> **Note:** When using an index in a join, it is no longer a "narrow" transformation. The index must first retrieve matching values from its records, load the appropriate data, and then perform the narrow transformation on the resulting DataFrame.  

### Example Usage  
```xml
<dependency>
    <groupId>dev.cjfravel</groupId>
    <artifactId>ariadne</artifactId>
    <version>0.0.1-alpha-8</version>
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
Context.spark = spark

// Create and configure the index
val index = Index("table", tableSchema, "parquet")
index.addIndex("version")
index.addFile(files: _*)
index.update

// Use the index in the join
val dfJoinedAgainstIndex = otherDf.join(index, Seq("version", "id"), "left_semi") // records in otherDf that have matching data in indexed files
val indexJoinedAgainstDf = index.join(otherDf, Seq("version", "id"), "left_semi") // matching data that has been indexed that is in otherDf
```