package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema provided for an index operation does not match
  * the schema stored in the existing index metadata.
  *
  * Raised by `Index.apply` when reconnecting to an existing index and the
  * provided schema differs from the persisted one, with
  * `allowSchemaMismatch = false` (the default).
  *
  * '''Recovery:''' Use the same schema as stored in the index, or pass
  * `allowSchemaMismatch = true` to `Index.apply` to update the stored
  * schema. Note that allowing schema mismatch still requires all indexed
  * columns to exist in the new schema.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * val schema1 = StructType(Seq(StructField("id", IntegerType)))
  * Index("myIndex", schema1, "parquet")
  * val schema2 = StructType(Seq(StructField("id", StringType)))
  * // Throws SchemaMismatchException — type changed from Int to String
  * Index("myIndex", schema2, "parquet")
  * }}}
  *
  * @param indexName the name of the index with the schema conflict
  */
class SchemaMismatchException(indexName: String)
    extends AriadneException(
      s"Schema mismatch for index '$indexName': provided schema does not match stored schema. " +
        "Use allowSchemaMismatch=true to update the stored schema."
    )
