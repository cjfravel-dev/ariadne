# Ariadne – Copilot Instructions

## Overview

Ariadne is a Scala 2.12 / Apache Spark library that builds file-level indexes for data lake joins. Index metadata and data are stored as Delta Lake tables on a Hadoop-accessible filesystem. The library is published for two Spark versions (3.4 and 3.5) via Maven profiles.

## Build & Test

```bash
# Build and test (default: Spark 3.4 profile)
mvn test

# Build and test with Spark 3.5
mvn test -P spark35

# Run a single test suite
mvn test -Dsuites="dev.cjfravel.ariadne.IndexTests"

# Run a specific test within a suite
mvn test -Dsuites="dev.cjfravel.ariadne.IndexTests#my test name"

# Package (produces shaded jar)
mvn package
```

The `test` phase also runs `dev/scripts/readme-has-version.sh`, which fails if the version in `pom.xml` is not present in `README.md`. Always update `README.md` when bumping the version.

Formatting: scalafmt (`runner.dialect = scala213`). Run with `scalafmt` or via your editor.

## Architecture

### Trait Hierarchy

`Index` (case class, public API) extends a stack of traits, each adding a layer:

```
Index
  └── IndexQueryOperations   # locateFiles (with range/auto-bloom pre-filtering), stats, printIndex
        └── IndexJoinOperations  # join, joinDf, temporal dedup
              └── IndexBuildOperations  # update, batching, staging, large indexes
                    └── BloomFilterOperations  # bloom filter build & query
                          └── IndexMetadataOperations  # read/write metadata.json
                                └── AriadneContextUser  # SparkSession, HDFS, config
```

All traits use `self: Index =>` to access `Index` members.

### Storage Layout (under `spark.ariadne.storagePath/{indexName}/`)

| Path | Purpose |
|------|---------|
| `metadata.json` | JSON config: schema, format, index types, read options |
| `index/` | Main Delta table — arrays of indexed values keyed by filename, plus range structs and auto-bloom binary columns |
| `staging/` | Transient Delta table during batched `update`; merged then deleted |
| `large_indexes/{column}/` | Separate Delta table for columns exceeding `largeIndexLimit` (exploded rows instead of arrays) |
| `.filelist.lock` / `.update.lock` | JSON lock files for concurrency control |

The file list (`{indexName}.json`) is tracked separately via `FileList`.

### Data Flow for `update`

1. `analyzeFiles` — pre-flight distinct-count scan to decide batching
2. `createOptimalBatches` — groups files so no batch exceeds `largeIndexLimit`
3. Per batch: read files → apply computed indexes → build regular/exploded/bloom/temporal/range index DataFrames → build auto-bloom filters for large columns → join them → `appendToStaging` + `appendToLargeIndex`
4. After every `stagingConsolidationThreshold` batches: `consolidateStaging` (Delta merge staging → main index); optionally auto-compact if `autoCompactThreshold` is set
5. Final consolidation at end of all batches

### Data Flow for `join`

`Index.join(df, cols)` → `joinDf` → `locateFilesFromDataFrame` (returns a `Set[String]` of matching file paths via intersection across all index types: regular, bloom, temporal, range, and auto-bloom pre-filtering) → `readFiles(files)` → `applyTemporalDeduplication` → `.join(df, cols, joinType)`

`df.join(index, cols, joinType)` (via the `DataFrameOps` implicit) is the reverse direction.

### Data Flow for `deleteFiles`

`deleteFiles(filenames)` → acquire update lock → merge-delete from main index → merge-delete from each large index table → merge-delete from staging (if exists) → remove from FileList → release lock

### Data Flow for `compact` / `vacuum`

`compact()` → acquire update lock → Delta OPTIMIZE on main index + all large index tables → release lock
`vacuum(hours)` → acquire update lock → Delta VACUUM on main index + all large index tables → release lock

## Key Conventions

### Index Type Mutual Exclusivity

A column can have exactly one index type: regular, bloom, computed, temporal, exploded, or range. Attempting to add a second type throws `IllegalArgumentException`. All add methods are idempotent (calling twice is a no-op).

### Metadata Versioning

`IndexMetadata.apply(jsonString)` performs sequential null-checks to migrate old metadata files forward (v1→v2→…→v9). When adding a new field to `IndexMetadata`, add a corresponding null-check migration block in `IndexMetadata.apply`.

### Logging Convention

`logger.warn(...)` is used for all normal operational messages (progress, counts, paths). This is intentional — it matches how Spark surfaces log output in cluster environments. Reserve `logger.debug` for verbose/low-value entries.

### Java/Scala Collections

`IndexMetadata` uses Java collections (`util.List`, `util.Map`) for Gson compatibility. Use `import scala.collection.JavaConverters._` and `.asScala` / `.asJava` when bridging. Never change `IndexMetadata` fields to Scala collections without updating Gson serialization.

### Shading

Guava and Gson are shaded under `dev.cjfravel.ariadne.shaded.*` to avoid version conflicts with the host Spark environment. Source code uses standard `com.google.common.*` (Guava) and `com.google.gson.*` (Gson) imports — the Maven Shade Plugin automatically relocates these to `dev.cjfravel.ariadne.shaded.*` in the packaged JAR. Do not add additional unshaded `com.google.*` runtime dependencies to `pom.xml` as they would conflict with the shading configuration.

### Spark Profiles

Default Maven profile is `spark34`. Use `-P spark35` for Spark 3.5 / Delta 3.2. The artifact ID encodes the Spark version: `ariadne-spark34_2.12` or `ariadne-spark35_2.12`.

### Tests

All Spark tests mix in `SparkTests`, which creates a local `SparkSession` with a temp directory as `spark.ariadne.storagePath` and the Delta extensions configured. Test data files live in `src/test/resources/` and are accessed via `resourcePath(fileName)`.

### Java Version

Java 11 is required (`JAVA_HOME=/usr/lib/jvm/java-11-openjdk`). Do not use Java features beyond Java 11. The Scala version (2.12.17) and Spark versions (3.4/3.5) are tied to Azure Synapse dependencies and must not be changed.

### Scaladoc Requirements

All classes, traits, objects, and public/protected methods must have scaladoc documentation:
- Class/trait/object level: purpose, thread-safety notes, relationship to other components
- Method level: `@param`, `@return`, `@throws` annotations
- Include usage examples for public API methods
- Document edge cases and limitations (e.g., driver OOM risks for `collect()`)
- Exception classes: document when thrown, by which methods, and recovery steps

### Logging Standards

- Use Log4j2 API (`org.apache.logging.log4j`)
- `logger.warn(...)` for all operational messages (progress, counts, paths, durations). This is intentional — Spark clusters surface warn-level output.
- `logger.debug(...)` for verbose/diagnostic entries only
- All public mutating methods (`update`, `compact`, `vacuum`, `deleteFiles`) must:
  - Log entry with index name and key parameters
  - Log completion with duration
  - Use catch-log-rethrow pattern so failures are logged before lock release
- All `add*Index` methods must log confirmation of the addition
- Silent catch blocks are prohibited — always log the exception before swallowing or rethrowing
- Include enough context in log messages for debugging (index name, file counts, column names, durations)

### Edge Case & Production Readiness Standards

- Guard `.head`, `.last`, `.get` calls with `nonEmpty`/`isDefined` checks
- Use try-finally for resource cleanup (broadcast variables, cached DataFrames, streams)
- Close all streams (ByteArrayOutputStream, ByteArrayInputStream, scala.io.Source) in finally blocks
- Guard against null in finally blocks (e.g., `if (stream != null) stream.close()`)
- Document thread-safety: `Index` instances are NOT safe for concurrent `select()` + `join()` from multiple threads
- Add comments documenting TOCTOU race conditions and driver OOM risks where applicable
- Validate inputs: throw `IllegalArgumentException` for null/empty required parameters

### Scala Coding Standards

- Expression-oriented style: use `if/else` expressions instead of `return` statements
- Use `import scala.collection.JavaConverters._` (NOT `CollectionConverters` which is Scala 2.13+)
- Prefer immutable collections; use `var` only when required (e.g., Gson compatibility in `IndexMetadata`)
- Use `Set.empty`, `Seq.empty` instead of `Set()`, `Seq()`
- String interpolation (`s"..."`) over concatenation
- Follow standard Scala naming: camelCase for methods/vals, PascalCase for types, UPPER_SNAKE for constants

### Spark Coding Standards

- Minimize driver-side `collect()` — document OOM risks where unavoidable
- Use try-finally for `broadcast.destroy()` and `df.unpersist()` cleanup
- Prefer built-in Spark functions over UDFs where possible
- Use `@transient` for non-serializable fields in closures
- Document any operations that break Catalyst optimization
