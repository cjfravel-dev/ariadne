package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema provided for an index operation does not match
  * the schema stored in the existing index metadata.
  *
  * Raised by `Index.apply` when reconnecting to an existing index and the
  * provided schema differs from the persisted one, with
  * `allowSchemaMismatch = false` (the default).
  *
  * {{{
  * val schema1 = StructType(Seq(StructField("id", IntegerType)))
  * Index("myIndex", schema1, "parquet")
  * val schema2 = StructType(Seq(StructField("id", StringType)))
  * // Throws SchemaMismatchException — type changed from Int to String
  * Index("myIndex", schema2, "parquet")
  * }}}
  */
class SchemaMismatchException
    extends AriadneException("Schema provided does not match stored schema")
