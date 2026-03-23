package dev.cjfravel.ariadne.exceptions

/** Thrown when the data format provided for an index operation does not match
  * the format stored in the index metadata.
  *
  * Raised by `Index.apply` when reconnecting to an existing index with a
  * different format than what was originally configured (e.g., creating an
  * index as "parquet" then attempting to use it as "json").
  *
  * {{{
  * // First creation stores format = "parquet"
  * Index("myIndex", schema, "parquet")
  * // Throws FormatMismatchException — format conflict
  * Index("myIndex", schema, "json")
  * }}}
  */
class FormatMismatchException
    extends AriadneException("Format provided does not match stored format")
