package dev.cjfravel.ariadne.exceptions

/** Thrown when a specified column is not found in the DataFrame schema or index
  * configuration.
  *
  * This exception is raised by index build and query operations when a column
  * referenced in the index configuration does not exist in the data schema.
  * Typical callers include `Index.addIndex`, `Index.addBloomIndex`,
  * `Index.addTemporalIndex`, and `Index.addRangeIndex`.
  *
  * {{{
  * // Throws ColumnNotFoundException if "nonexistent_col" is not in the schema
  * index.addIndex("nonexistent_col")
  * }}}
  *
  * @param column The name of the column that was not found
  */
class ColumnNotFoundException(column: String) extends AriadneException(s"Column $column was not found in the dataframe")