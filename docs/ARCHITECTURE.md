# Ariadne Index Architecture

## Overview

The Ariadne Index system provides a modular architecture for managing file-based indexes in Apache Spark using Delta Lake. The architecture uses Scala traits to organize functionality into cohesive, testable modules with clear separation of concerns.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          Index Class                            │
│                     (Main Public API)                           │
│                    case class Index                             │
├─────────────────────────────────────────────────────────────────┤
│  Extends: IndexQueryOperations                                  │
│                                                                 │
│  Trait Inheritance Chain:                                       │
│  IndexQueryOperations                                           │
│    ↳ IndexJoinOperations                                        │
│        ↳ IndexBuildOperations                                   │
│            ↳ BloomFilterOperations                              │
│                ↳ IndexFileOperations                            │
│                    ↳ IndexMetadataOperations                    │
│                        ↳ AriadneContextUser                     │
└─────────────────────────────────────────────────────────────────┘
                               │
                               │ uses
                               ▼
┌─────────────────────┐    ┌─────────────────────┐
│   IndexPathUtils    │    │     FileList        │
│ (Utility Object)    │    │  (File Tracking)    │
└─────────────────────┘    └─────────────────────┘
```

## Core Components

### 1. AriadneContextUser (Base Trait)
**Purpose**: Provides shared access to SparkSession and Hadoop FileSystem operations.

**Key Responsibilities**:
- SparkSession management through AriadneContext
- Hadoop filesystem operations (exists, delete, open, etc.)
- Storage path configuration
- Delta table access utilities

**Key Methods**:
- `spark: SparkSession` - Access to Spark session
- `fs: FileSystem` - Hadoop filesystem interface
- `storagePath: Path` - Base storage path for Ariadne data
- `exists(path: Path): Boolean` - Check path existence
- `delete(path: Path): Boolean` - Delete path
- `delta(path: Path): Option[DeltaTable]` - Access Delta table

### 2. IndexMetadataOperations
**Purpose**: Handles all metadata-related operations including persistence, validation, and caching.

**Key Responsibilities**:
- Schema management and JSON serialization
- Format validation (csv, json, parquet)
- Metadata persistence to JSON files
- Metadata caching and refresh
- Index configuration management

**Public Methods**:
- `format: String` - Returns the file format
- `refreshMetadata(): Unit` - Refreshes cached metadata from disk

**Protected Methods**:
- `metadata: IndexMetadata` - Access to cached metadata object
- `writeMetadata(metadata: IndexMetadata): Unit` - Persists metadata to disk
- `metadataExists: Boolean` - Checks if metadata file exists
- `metadataFilePath: Path` - Path to metadata file

### 3. IndexFileOperations
**Purpose**: Manages file reading, DataFrame creation, and data transformations.

**Key Responsibilities**:
- Reading files in different formats (CSV, JSON, Parquet)
- Applying read options for format-specific configuration
- Creating DataFrames with computed columns
- Handling exploded field transformations
- Schema enforcement

**Key Methods**:
- `storedSchema: StructType` - Returns the stored schema

**Protected Methods**:
- `readFiles(files: Set[String]): DataFrame` - Reads files into DataFrame
- `createBaseDataFrame(files: Set[String]): DataFrame` - Creates base DataFrame
- `applyComputedIndexes(df: DataFrame): DataFrame` - Adds computed columns
- `applyExplodedFields(df: DataFrame): DataFrame` - Adds exploded field columns

### 4. BloomFilterOperations
**Purpose**: Provides bloom filter operations for space-efficient probabilistic indexing of high-cardinality columns.

**Key Responsibilities**:
- Building bloom filters during index updates
- Serializing/deserializing bloom filters to/from binary format
- Querying bloom filters for file location
- Managing bloom filter configurations (column, FPR)

**Key Concepts**:
- **No false negatives**: If the filter says "no", the value is definitely not present
- **Configurable false positives**: If the filter says "yes", the value MIGHT be present (controlled by FPR)
- **Space efficiency**: Approximately 10 bits per element at 1% false positive rate

**Protected Methods**:
- `buildBloomFilterIndexes(df: DataFrame): DataFrame` - Creates binary bloom filter columns
- `locateFilesWithBloom(column: String, values: Array[Any], indexDf: DataFrame): Set[String]` - Finds files using bloom filter
- `bloomMightContain(bloomBytes: Array[Byte], value: Any): Boolean` - Checks if value might be in filter
- `deserializeBloomFilter(bytes: Array[Byte]): BloomFilter[CharSequence]` - Deserializes bloom filter
- `bloomColumns: Set[String]` - Returns columns with bloom indexes
- `bloomStorageColumns: Set[String]` - Returns storage column names (with `bloom_` prefix)

### 5. IndexBuildOperations
**Purpose**: Handles the core index building logic and Delta table operations.

**Key Responsibilities**:
- Building regular column indexes
- Building computed indexes
- Building exploded field indexes
- Managing large index thresholds and separate storage
- Delta table operations for index storage
- Incremental updates and merges

**Protected Methods**:
- `buildRegularIndexes(df: DataFrame): DataFrame` - Builds regular indexes
- `buildExplodedFieldIndexes(baseData: DataFrame, resultDf: DataFrame): DataFrame` - Builds exploded indexes
- `handleLargeIndexes(df: DataFrame): Unit` - Handles large index storage
- `mergeToDelta(df: DataFrame): Unit` - Merges data to Delta table
- `storageColumns: Set[String]` - Returns all storage column names
- `indexFilePath: Path` - Path to main index Delta table
- `largeIndexesFilePath: Path` - Path to large indexes storage

### 6. IndexJoinOperations
**Purpose**: Provides DataFrame join functionality using the index for optimization with caching support.

**Key Responsibilities**:
- Performing optimized joins using index data
- Supporting different join types (inner, left_semi, full_outer)
- Join result caching for performance
- Multi-column join support
- Column mapping for exploded fields and bloom filter columns

**Key Methods**:
- `join(df: DataFrame, usingColumns: Seq[String], joinType: String = "inner"): DataFrame`

**Protected Methods**:
- `joinDf(df: DataFrame, usingColumns: Seq[String]): DataFrame` - Creates optimized join DataFrame
- `mapJoinColumnsToStorage(joinColumns: Seq[String]): Map[String, String]` - Maps join to storage columns (including bloom columns with `bloom_` prefix)
- `applyJoinFilters(...)` - Creates filter conditions (skips bloom columns since filtering is done at file level)

### 7. IndexQueryOperations
**Purpose**: Handles file location queries, statistics generation, and index introspection.

**Key Responsibilities**:
- Locating files based on index values
- Generating statistics for indexed columns
- Index printing and debugging utilities
- Multi-criteria file lookup
- Large index integration

**Key Methods**:
- `locateFiles(indexes: Map[String, Array[Any]]): Set[String]` - Find files matching criteria
- `stats(): DataFrame` - Get comprehensive statistics
- `printIndex(truncate: Boolean = false): Unit` - Print index contents
- `printMetadata: Unit` - Print metadata information

**Protected Methods**:
- `index: Option[DataFrame]` - Access to complete index DataFrame with large index integration

### 8. Index Class (Main API)
**Purpose**: Main public interface that combines all functionality through trait composition.

**Constructor**:
```scala
case class Index private (
  name: String,
  schema: Option[StructType]
)
```

**Key Public Methods**:
- `hasFile(fileName: String): Boolean` - Check if file is tracked
- `addFile(fileNames: String*): Unit` - Add files to tracking
- `addIndex(index: String): Unit` - Add regular column index
- `addBloomIndex(column: String, fpr: Double = 0.01): Unit` - Add bloom filter index (mutually exclusive with regular index)
- `addComputedIndex(name: String, sql_expression: String): Unit` - Add computed index
- `addExplodedFieldIndex(arrayColumn: String, fieldPath: String, asColumn: String): Unit` - Add exploded field index
- `indexes: Set[String]` - Get all available index column names (includes both regular and bloom indexes)
- `update: Unit` - Update index with new files
- `storagePath: Path` - Storage location for this index

**Factory Methods** (in companion object):
- `Index(name: String, schema: StructType, format: String): Index`
- `Index(name: String, schema: StructType, format: String, allowSchemaMismatch: Boolean): Index`
- `Index(name: String, schema: StructType, format: String, readOptions: Map[String, String]): Index`

### 9. IndexPathUtils (Object)
**Purpose**: Provides utility functions for path manipulation and index management.

**Key Responsibilities**:
- File name cleaning and sanitization
- Index existence checking
- Index removal operations
- Storage path management

**Methods**:
- `cleanFileName(fileName: String): String` - Sanitizes file names for storage
- `fileListName(name: String): String` - Generates FileList names
- `exists(name: String): Boolean` - Checks if index exists
- `remove(name: String): Boolean` - Removes an index
- `storagePath: Path` - Overrides base storage path for indexes

### 10. Supporting Classes

#### IndexMetadata
**Purpose**: Data container for index configuration and state with version migration support.

**Fields**:
- `format: String` - File format (csv, json, parquet)
- `schema: String` - JSON representation of DataFrame schema
- `indexes: util.List[String]` - Regular column indexes
- `computed_indexes: util.Map[String, String]` - Computed index expressions
- `exploded_field_indexes: util.List[ExplodedFieldMapping]` - Exploded field configurations
- `read_options: util.Map[String, String]` - Format-specific read options
- `bloom_indexes: util.List[BloomIndexConfig]` - Bloom filter index configurations

#### BloomIndexConfig
**Purpose**: Configuration for bloom filter indexes on high-cardinality columns.

**Fields**:
- `column: String` - Column name to index
- `fpr: Double` - False positive rate (default: 0.01 = 1%)

#### ExplodedFieldMapping
**Purpose**: Configuration for indexing fields within array columns.

**Fields**:
- `array_column: String` - Array column name
- `field_path: String` - Field path within array elements
- `as_column: String` - Alias for join operations

#### FileList
**Purpose**: Tracks which files have been added to an index for processing.

## Supported File Formats

Ariadne supports three data formats with Parquet as the default:
- **Parquet** - Columnar format (default, recommended)
- **CSV** - Comma-separated values
- **JSON** - JavaScript Object Notation

Additional format-specific options can be provided via `readOptions` when creating an index.

## Key Features

### Large Index Handling
When indexes become too large (exceeding `largeIndexLimit`), they are automatically stored in consolidated Delta tables under the `large_indexes` directory. The system uses a consolidated approach where each column gets its own Delta table (e.g., `large_indexes/user_id`) containing all filename/value combinations for that column. This provides better performance than the previous approach of creating separate tables per file.

**Consolidated Storage Structure:**
```
large_indexes/
├── user_id/           (Single Delta table: filename, user_id - ALL files)
├── category/          (Single Delta table: filename, category - ALL files)
└── ...
```

**Benefits:**
- **Better Delta Lake Performance**: Single optimized table per column vs. many small tables
- **Improved File Management**: Delta can optimize file sizes and compaction more effectively
- **Enhanced Data Skipping**: Z-ordering and data skipping work better on consolidated data
- **Simplified Queries**: Direct table scans instead of complex union operations

### Staged Append + Consolidation Strategy
When processing large numbers of files, the system uses a staged append strategy to improve performance by reducing the number of expensive merge operations.

**Storage Structure:**
```
index_name/
├── index/            (Main consolidated index table)
├── staging/          (Temporary staging table during batch processing)
└── large_indexes/    (Large index tables - direct append, no staging)
```

**Main Index Flow (uses staging):**
1. **Batch Processing**: Each batch appends to `staging/`
2. **Periodic Consolidation**: Every N batches (configurable via `spark.ariadne.stagingConsolidationThreshold`, default: 50), staging is merged to main index
3. **Final Consolidation**: At the end of `update()`, any remaining staged data is consolidated and staging is deleted

**Large Index Flow (no staging):**
- Large indexes write directly to `large_indexes/{column}` via append
- Data is deduplicated within each batch before writing

**Configuration:**
- `spark.ariadne.stagingConsolidationThreshold`: Number of batches before consolidation (default: 50)

**Benefits:**
- **Reduced Merge Operations**: Batches are appended quickly, with merges only at consolidation points
- **Fault Tolerance**: Periodic consolidation preserves work in case of job failure
- **Memory Efficient**: Avoids accumulating large in-memory structures

**Performance Comparison:**
| Batches | Without Staging | With Staging |
|---------|-----------------|--------------|
| 50      | ~1,275 merges   | 1 merge      |
| 100     | ~5,050 merges   | 2 merges     |
| 180     | ~16,290 merges  | 4 merges     |

### Delta Lake Integration
All index data is stored using Delta Lake format, providing:
- ACID transactions
- Time travel capabilities
- Schema evolution
- Efficient upserts and merges

### Bloom Filter Indexes
Bloom filters provide space-efficient probabilistic indexing for high-cardinality columns (like user IDs, transaction IDs). They are ideal for columns where:
- The number of distinct values per file is very large
- Regular indexes would consume too much storage
- Some false positives are acceptable in exchange for significant space savings

**Storage:**
- Bloom filters are stored as binary columns in the main index Delta table with `bloom_` prefix
- Example: A bloom index on `user_id` creates a `bloom_user_id` column containing serialized Guava BloomFilter bytes

**Query Behavior:**
- File location uses probabilistic matching (may return files that don't actually contain the value)
- No additional row-level filtering is applied since only the bloom filter exists (not the actual values)
- False positives result in reading slightly more files than strictly necessary

**Configuration:**
- `fpr` (false positive rate): Controls accuracy vs. size tradeoff (default: 0.01 = 1%)
- Lower FPR = more accurate but larger storage; Higher FPR = smaller but more false positives

**Mutual Exclusivity:**
- A column can have either a regular index OR a bloom index, not both
- This is enforced at the API level with clear error messages

### Metadata Versioning
The system supports automatic migration between metadata versions:
- v1 → v2: Adds computed_indexes support
- v2 → v3: Adds exploded_field_indexes support
- v3 → v4: Adds read_options support
- v4 → v5: Adds bloom_indexes support

### Caching Strategy
Join operations utilize intelligent caching to avoid recomputing expensive operations when the same files and filter criteria are used repeatedly.

## Design Benefits

### 1. **Trait-Based Modularity**
The linear inheritance chain ensures each trait builds upon the previous one's functionality while maintaining clear separation of concerns.

### 2. **Testability**
Each trait can be tested independently with focused test suites:
- `IndexMetadataOperationsTests` - Tests metadata handling
- `IndexFileOperationsTests` - Tests file operations
- `IndexBuildOperationsTests` - Tests index building
- `IndexJoinOperationsTests` - Tests join functionality
- `IndexQueryOperationsTests` - Tests querying and statistics
- `IndexPathUtilsTests` - Tests utility functions
- `BloomFilterOperationsTests` - Tests bloom filter functionality

### 3. **Performance Optimization**
- Large index handling prevents memory overflow
- Join caching reduces repeated computations
- Delta Lake provides efficient storage and querying
- Exploded field indexing enables efficient nested data joins
- Bloom filter indexes provide space-efficient probabilistic matching for high-cardinality columns

### 4. **Extensibility**
New functionality can be added by extending existing traits or creating new ones in the inheritance chain.

### 5. **Schema Evolution**
Supports schema changes with configurable validation and automatic metadata migration.

## Working with the Architecture

### Adding New Functionality

1. **For metadata operations**: Extend `IndexMetadataOperations`
2. **For file operations**: Extend `IndexFileOperations`
3. **For bloom filter operations**: Extend `BloomFilterOperations`
4. **For index building**: Extend `IndexBuildOperations`
5. **For join operations**: Extend `IndexJoinOperations`
6. **For query operations**: Extend `IndexQueryOperations`
7. **For utilities**: Add to `IndexPathUtils` object

### Testing Strategy

Each module has focused tests that verify specific functionality:

```scala
// Example: Testing join operations
class IndexJoinOperationsTests extends SparkTests {
  test("join should cache results for repeated operations") {
    val index = Index("test", schema, "parquet")
    index.addIndex("id")
    // Test implementation...
  }
}
```

### Error Handling

Each module handles specific error conditions:
- `MetadataMissingOrCorruptException` - Metadata issues
- `SchemaMismatchException` - Schema validation failures
- `FormatMismatchException` - Format validation failures
- `IndexNotFoundException` - Missing index references
- `IndexNotFoundInNewSchemaException` - Schema evolution issues

## Implementation Details

### Trait Composition Pattern
The Index class uses a linear inheritance chain rather than multiple trait mixing:

```scala
case class Index private (
  name: String,
  schema: Option[StructType]
) extends IndexQueryOperations {
  // IndexQueryOperations extends IndexJoinOperations
  // IndexJoinOperations extends IndexBuildOperations
  // IndexBuildOperations extends BloomFilterOperations
  // BloomFilterOperations extends IndexFileOperations
  // IndexFileOperations extends IndexMetadataOperations
  // IndexMetadataOperations extends AriadneContextUser
}
```

### Self-Type Annotations
Each trait uses self-type annotations to ensure proper composition:

```scala
trait IndexMetadataOperations extends AriadneContextUser {
  self: Index =>
  // Implementation that can access Index fields and methods
}
```

### Factory Method Pattern
The companion object provides multiple factory methods to handle different initialization scenarios with proper metadata validation and migration.

### Dependency Management
- All traits ultimately depend on `AriadneContextUser` for Spark and filesystem access
- `FileList` is used independently for file tracking
- `IndexPathUtils` provides shared utilities across the system
- Delta Lake integration is handled through `AriadneContextUser`

This architecture provides a robust, scalable foundation for file-based indexing in Spark while maintaining clean separation of concerns and excellent testability.
