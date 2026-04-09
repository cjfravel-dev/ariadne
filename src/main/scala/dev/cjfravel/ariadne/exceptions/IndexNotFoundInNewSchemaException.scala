package dev.cjfravel.ariadne.exceptions

/** Thrown when a previously indexed column is not found in the new schema during
  * a schema evolution check.
  *
  * Raised by `Index.apply` when `allowSchemaMismatch = true` and the provided
  * schema is missing a column that the existing index tracks. When schema
  * mismatch is allowed, Ariadne still validates that all indexed columns exist
  * in the new schema to prevent silent data loss where an indexed column would
  * become inaccessible.
  *
  * '''Recovery:''' Add the missing column to the new schema, or remove the
  * index on that column (via `Index.removeIndex`) before changing the schema.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // Original index has column "user_id"
  * val newSchema = StructType(Seq(StructField("name", StringType)))
  * // Throws IndexNotFoundInNewSchemaException — "user_id" is missing
  * Index("myIndex", newSchema, "parquet", allowSchemaMismatch = true)
  * }}}
  *
  * @param col The name of the indexed column missing from the new schema
  */
class IndexNotFoundInNewSchemaException(col: String)
    extends AriadneException(s"Index $col was not found in new schema")
