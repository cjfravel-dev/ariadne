package dev.cjfravel.ariadne.exceptions

/** Thrown when an index does not have a file format specified in its metadata.
  *
  * Raised by `Index.apply` when creating a new index without providing a
  * format, or when the existing `IndexMetadata.format` is null or empty
  * (indicating a corrupt or manually edited metadata file).
  *
  * '''Recovery:''' Provide the `format` parameter when creating a new index
  * (e.g., `Index("myIndex", schema, "parquet")`). If the metadata is corrupt,
  * delete and re-create the index.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // Throws MissingFormatException if no format is provided for a new index
  * Index("newIndex", schema)  // missing format parameter
  * }}}
  */
class MissingFormatException
    extends AriadneException("Index doesn't have a specified fileformat")
