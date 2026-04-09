package dev.cjfravel.ariadne.exceptions

/** Thrown when the data format provided for an index operation does not match
  * the format stored in the index metadata.
  *
  * Raised by `Index.apply` when reconnecting to an existing index with a
  * different format than what was originally configured (e.g., creating an
  * index as "parquet" then attempting to use it as "json").
  *
  * '''Recovery:''' Use the correct format matching the stored metadata, or
  * delete and re-create the index with the desired format. Call
  * `IndexCatalog.describe(name)` to inspect the stored format.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // First creation stores format = "parquet"
  * Index("myIndex", schema, "parquet")
  * // Throws FormatMismatchException — format conflict
  * Index("myIndex", schema, "json")
  * }}}
  *
  * @param indexName the name of the index with the format conflict
  * @param expected  the format stored in existing metadata
  * @param actual    the format that was provided and does not match
  */
class FormatMismatchException(indexName: String, expected: String, actual: String)
    extends AriadneException(
      s"Format mismatch for index '$indexName': stored format is '$expected' but '$actual' was provided"
    )
