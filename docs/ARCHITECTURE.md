# Ariadne Index Class Architecture

## Overview

The Ariadne Index class has been refactored into a modular architecture using Scala traits to improve code organization, maintainability, and testability. This document describes the new design and how to work with it.

## Architecture Diagram



┌─────────────────────────────────────────────────────────────────┐
│                          Index Class                           │
│                     (Main Public API)                          │
├─────────────────────────────────────────────────────────────────┤
│  Extends:                                                       │
│  • IndexMetadataOperations                                      │
│  • IndexFileOperations                                          │
│  • IndexBuildOperations                                         │
│  • IndexJoinOperations                                          │
│  • IndexQueryOperations                                         │
└─────────────────────────────────────────────────────────────────┘
│
│ uses
▼
┌─────────────────────┐
│   IndexPathUtils    │
│ (Utility Functions) │
└─────────────────────┘


## Module Breakdown

### 1. IndexMetadataOperations
**Purpose**: Handles all metadata-related operations including persistence, validation, and caching.

**Key Responsibilities**:
- Schema management and validation
- Format validation
- Metadata persistence to JSON files
- Metadata caching and refresh
- Index configuration management

**Public Methods**:
- `format: String` - Returns the file format (csv, json, etc.)
- `storedSchema: StructType` - Returns the stored schema
- `indexes: Array[String]` - Returns list of all index names
- `refreshMetadata()` - Refreshes cached metadata from disk

**Protected Methods**:
- `metadata: IndexMetadata` - Access to metadata object
- `writeMetadata()` - Persists metadata to disk
- `readMetadata(): IndexMetadata` - Reads metadata from disk

### 2. IndexFileOperations
**Purpose**: Manages file reading, DataFrame creation, and data transformations.

**Key Responsibilities**:
- Reading files in different formats (CSV, JSON)
- Applying read options
- Creating DataFrames with computed columns
- Handling exploded field transformations
- File validation and schema enforcement

**Key Methods**:
- `addFile(path: String)` - Adds a file to the index
- `hasFile(path: String): Boolean` - Checks if file exists in index
- `createDataFrame(): DataFrame` - Creates DataFrame from indexed files

**Protected Methods**:
- `applyReadOptions(reader: DataFrameReader): DataFrameReader`
- `applyComputedIndexes(df: DataFrame): DataFrame`
- `applyExplodedFieldTransformations(df: DataFrame): DataFrame`

### 3. IndexBuildOperations
**Purpose**: Handles the core index building logic and update operations.

**Key Responsibilities**:
- Building regular column indexes
- Building computed indexes
- Building exploded field indexes
- Managing large index thresholds
- Delta table operations for index storage
- Incremental updates

**Key Methods**:
- `update()` - Updates the index with new/changed files
- `addIndex(columnName: String)` - Adds a regular column index
- `addComputedIndex(name: String, expression: String)` - Adds computed index
- `addExplodedFieldIndex(arrayColumn: String, fieldPath: String, asColumn: String)` - Adds exploded field index

**Protected Methods**:
- `buildRegularIndexes(df: DataFrame): DataFrame`
- `buildExplodedFieldIndexes(baseData: DataFrame, resultDf: DataFrame): DataFrame`
- `handleLargeIndexes(df: DataFrame): Unit`
- `mergeToDelta(df: DataFrame): Unit`

### 4. IndexJoinOperations
**Purpose**: Provides DataFrame join functionality using the index for optimization.

**Key Responsibilities**:
- Performing optimized joins using index data
- Supporting different join types (inner, left_semi, full_outer)
- Join result caching for performance
- Multi-column join support

**Key Methods**:
- `join(other: DataFrame, joinColumns: Seq[String], joinType: String = "inner"): DataFrame`

**Protected Methods**:
- `performJoin(other: DataFrame, joinColumns: Seq[String], joinType: String): DataFrame`

### 5. IndexQueryOperations
**Purpose**: Handles file location queries, statistics generation, and index introspection.

**Key Responsibilities**:
- Locating files based on index values
- Generating statistics for indexed columns
- Index printing and debugging utilities
- Multi-criteria file lookup

**Key Methods**:
- `locateFiles(criteria: Map[String, Array[Any]]): Set[String]` - Find files matching criteria
- `statistics(columnName: String): Map[String, Any]` - Get column statistics
- `printIndex(columnName: String = "", truncate: Boolean = true)` - Print index contents
- `printMetadata()` - Print metadata information

### 6. IndexPathUtils (Object)
**Purpose**: Provides utility functions for path manipulation and index management.

**Key Responsibilities**:
- File name cleaning and sanitization
- Path generation for storage
- Index existence checking
- Index removal operations

**Methods**:
- `cleanFileName(fileName: String): String` - Sanitizes file names for storage
- `fileListName(indexName: String): String` - Generates file list names
- `storagePath(basePath: String, indexName: String): String` - Generates storage paths
- `exists(indexName: String): Boolean` - Checks if index exists
- `remove(indexName: String): Unit` - Removes an index

## Design Benefits

### 1. **Separation of Concerns**
Each trait has a single, well-defined responsibility, making the code easier to understand and maintain.

### 2. **Testability**
Each module can be tested independently with focused test suites:
- `IndexMetadataOperationsTests` - Tests metadata handling
- `IndexFileOperationsTests` - Tests file operations
- `IndexBuildOperationsTests` - Tests index building
- `IndexJoinOperationsTests` - Tests join functionality
- `IndexQueryOperationsTests` - Tests querying and statistics
- `IndexPathUtilsTests` - Tests utility functions

### 3. **Maintainability**
Changes to specific functionality are isolated to their respective traits, reducing the risk of unintended side effects.

### 4. **Extensibility**
New functionality can be added by creating new traits or extending existing ones without modifying the core Index class.

### 5. **Code Reuse**
Common functionality is centralized in utility objects and can be reused across different components.

## Working with the New Architecture

### Adding New Functionality

1. **For metadata operations**: Extend `IndexMetadataOperations`
2. **For file operations**: Extend `IndexFileOperations`
3. **For index building**: Extend `IndexBuildOperations`
4. **For join operations**: Extend `IndexJoinOperations`
5. **For query operations**: Extend `IndexQueryOperations`
6. **For utilities**: Add to `IndexPathUtils` object

### Testing Strategy

Each module has its own test suite that focuses on testing that specific functionality:

```scala
// Example: Testing metadata operations
class IndexMetadataOperationsTests extends SparkTests {
  test("metadata should handle regular indexes") {
    val index = Index("test", schema, "csv")
    index.addIndex("column1")
    index.indexes should contain("column1")
  }
}
```



### Protected vs Public Methods
* **Public methods:** Part of the external API, used by consumers
* **Protected methods:** Internal implementation details, used by other traits

### Error Handling
Each module handles its own error conditions and throws appropriate exceptions:
* `MetadataMissingOrCorruptException` - Metadata issues
* `SchemaMismatchException` - Schema validation failures
* `FormatMismatchException` - Format validation failures
* `IndexNotFoundException` - Missing index references

## Implementation Details
### Trait Composition Pattern
The Index class uses Scala's trait composition to combine functionality:

```scala
class Index(
  val indexName: String,
  schema: Option[StructType] = None,
  format: Option[String] = None,
  readOptions: Map[String, String] = Map.empty,
  allowSchemaMismatch: Boolean = false
) extends IndexMetadataOperations 
    with IndexFileOperations 
    with IndexBuildOperations 
    with IndexJoinOperations 
    with IndexQueryOperations {
  // Implementation
}
```
### Self-Type Annotations
Each trait uses self-type annotations to ensure they're only mixed into Index instances:

```scala
trait IndexMetadataOperations {
  self: Index =>
  // Implementation that can access Index fields and methods
}
```

### Dependency Management
Traits are designed with clear dependencies:

* `IndexBuildOperations` extends `IndexFileOperations` (needs file operations)
* All traits depend on `IndexMetadataOperations` (implicitly through Index)
* Utility functions are centralized in `IndexPathUtils` object
