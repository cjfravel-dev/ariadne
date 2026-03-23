package dev.cjfravel.ariadne.exceptions

/** Thrown when a previously indexed column is not found in the new schema during
  * a schema evolution check.
  *
  * Raised by `Index.apply` when `allowSchemaMismatch = false` (the default) and
  * the provided schema is missing a column that the existing index tracks. This
  * prevents silent data loss where an indexed column would become inaccessible.
  *
  * {{{
  * // Original index has column "user_id"
  * val newSchema = StructType(Seq(StructField("name", StringType)))
  * // Throws IndexNotFoundInNewSchemaException — "user_id" is missing
  * Index("myIndex", newSchema, "parquet")
  * }}}
  *
  * @param col The name of the indexed column missing from the new schema
  */
class IndexNotFoundInNewSchemaException(col: String)
    extends AriadneException(s"Index $col was not found in new schema")
