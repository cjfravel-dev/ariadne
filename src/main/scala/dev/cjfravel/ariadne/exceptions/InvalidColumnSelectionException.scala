package dev.cjfravel.ariadne.exceptions

/** Exception thrown when attempting to select columns that don't exist in the schema
  * or haven't been previously selected.
  *
  * @param invalidColumns The column names that are invalid
  * @param availableColumns The columns that are available for selection
  */
class InvalidColumnSelectionException(
    invalidColumns: Set[String],
    availableColumns: Set[String]
) extends Exception(
      s"Invalid column selection. Columns [${invalidColumns.mkString(", ")}] are not available. " +
        s"Available columns: [${availableColumns.mkString(", ")}]"
    )