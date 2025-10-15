package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.InvalidColumnSelectionException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.Path

/** A read-only view of an Index with column selection applied.
  *
  * This class wraps an existing Index and restricts operations to only work with
  * a selected subset of columns. It prevents modification operations and filters
  * join operations to only use the selected columns.
  *
  * @param underlying The underlying Index instance
  * @param selectedColumns The set of columns that are selected for this view
  */
class SelectedIndex private[ariadne] (
    private val underlying: Index,
    val selectedColumns: Set[String]
)(implicit val spark: SparkSession) {

  // Delegate basic properties to underlying index
  val name: String = underlying.name
  val schema: Option[StructType] = underlying.schema
  lazy val storagePath: Path = underlying.storagePath

  /** Returns only the selected columns that are actually indexed */
  def indexes: Set[String] = underlying.indexes.intersect(selectedColumns)

  /** Validates that the requested columns are available for selection.
    *
    * @param columns The columns to validate
    * @param availableColumns The set of columns available for selection
    * @throws InvalidColumnSelectionException if any column is invalid
    */
  private def validateColumnSelection(columns: Set[String], availableColumns: Set[String]): Unit = {
    val invalidColumns = columns -- availableColumns
    if (invalidColumns.nonEmpty) {
      throw new InvalidColumnSelectionException(invalidColumns, availableColumns)
    }
  }

  /** Creates a new SelectedIndex with further column restriction.
    *
    * @param columns The columns to select (must be subset of currently selected columns)
    * @return A new SelectedIndex with the specified columns
    * @throws InvalidColumnSelectionException if any column is not in the current selection
    */
  def select(columns: String*): SelectedIndex = {
    val requestedColumns = columns.toSet
    validateColumnSelection(requestedColumns, selectedColumns)
    new SelectedIndex(underlying, requestedColumns)
  }

  /** Locates files based on index values, filtering to selected columns only.
    *
    * @param indexes A map of index column names to their values
    * @return A set of file names matching the criteria
    */
  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
    // Filter the indexes map to only include selected columns
    val filteredIndexes = indexes.filter { case (column, _) =>
      selectedColumns.contains(column)
    }
    underlying.locateFiles(filteredIndexes)
  }

  /** Returns statistics for the underlying index */
  def stats(): DataFrame = underlying.stats()

  /** Performs a join with the selected index, applying column filtering.
    *
    * @param df The DataFrame to join
    * @param usingColumns The columns to use for the join
    * @param joinType The type of join (default is "inner")
    * @return The resulting joined DataFrame with selected columns only
    */
  def join(
      df: DataFrame,
      usingColumns: Seq[String],
      joinType: String = "inner"
  ): DataFrame = {
    // Filter using columns to only those that are selected
    val filteredUsingColumns = usingColumns.filter(selectedColumns.contains)
    
    // Get the index DataFrame and apply column selection
    val indexDf = underlying.join(df, filteredUsingColumns, joinType)
    
    // Apply column selection to the result
    val columnsToSelect = (selectedColumns.intersect(indexDf.columns.toSet) ++ filteredUsingColumns).toSeq.distinct
    if (columnsToSelect.nonEmpty) {
      indexDf.select(columnsToSelect.map(indexDf.col): _*)
    } else {
      indexDf
    }
  }

  // Restricted operations - these should throw exceptions or be no-ops
  def hasFile(fileName: String): Boolean = underlying.hasFile(fileName)

  /** Adding files is not allowed on selected indexes */
  def addFile(fileNames: String*): Unit = {
    throw new UnsupportedOperationException("Cannot add files to a selected index. Use the original index instead.")
  }

  /** Adding indexes is not allowed on selected indexes */
  def addIndex(index: String): Unit = {
    throw new UnsupportedOperationException("Cannot add indexes to a selected index. Use the original index instead.")
  }

  /** Adding computed indexes is not allowed on selected indexes */
  def addComputedIndex(name: String, sql_expression: String): Unit = {
    throw new UnsupportedOperationException("Cannot add computed indexes to a selected index. Use the original index instead.")
  }

  /** Adding exploded field indexes is not allowed on selected indexes */
  def addExplodedFieldIndex(arrayColumn: String, fieldPath: String, asColumn: String): Unit = {
    throw new UnsupportedOperationException("Cannot add exploded field indexes to a selected index. Use the original index instead.")
  }

  /** Updating is not allowed on selected indexes */
  def update: Unit = {
    throw new UnsupportedOperationException("Cannot update a selected index. Use the original index instead.")
  }

  // Delegate read-only metadata operations
  def format: String = underlying.format
  def refreshMetadata(): Unit = underlying.refreshMetadata()

  // Debugging operations
  private[ariadne] def printIndex(truncate: Boolean = false): Unit = {
    underlying.printIndex(truncate)
  }

  private[ariadne] def printMetadata: Unit = underlying.printMetadata
}

/** Companion object for SelectedIndex */
object SelectedIndex {
  
  /** Creates a SelectedIndex from an underlying Index with column validation.
    *
    * @param underlying The underlying Index
    * @param columns The columns to select
    * @return A new SelectedIndex
    * @throws InvalidColumnSelectionException if any column is invalid
    */
  private[ariadne] def apply(underlying: Index, columns: Set[String]): SelectedIndex = {
    // Get all available columns from the schema plus computed and exploded field indexes
    val schemaColumns = underlying.schema match {
      case Some(structType) => structType.fieldNames.toSet
      case None => Set.empty[String]
    }
    
    // Include computed index columns and exploded field index columns
    val computedColumns = underlying.indexes
    val availableColumns = schemaColumns ++ computedColumns
    
    // Validate that all requested columns exist in the available columns
    val invalidColumns = columns -- availableColumns
    if (invalidColumns.nonEmpty) {
      throw new InvalidColumnSelectionException(invalidColumns, availableColumns)
    }
    
    new SelectedIndex(underlying, columns)(underlying.spark)
  }
}