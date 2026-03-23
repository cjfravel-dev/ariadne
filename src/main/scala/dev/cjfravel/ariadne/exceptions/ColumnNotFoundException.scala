package dev.cjfravel.ariadne.exceptions

/** Thrown when a specified column is not found in the DataFrame schema or index configuration.
  *
  * @param column The name of the column that was not found
  */
class ColumnNotFoundException(column: String) extends AriadneException(s"Column $column was not found in the dataframe")