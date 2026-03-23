package dev.cjfravel.ariadne.exceptions

/** Thrown when a previously indexed column is not found in the new schema during
  * a schema evolution check.
  *
  * @param col The name of the indexed column missing from the new schema
  */
class IndexNotFoundInNewSchemaException(col: String)
    extends AriadneException(s"Index $col was not found in new schema")
