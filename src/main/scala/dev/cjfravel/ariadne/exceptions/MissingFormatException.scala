package dev.cjfravel.ariadne.exceptions

/** Thrown when an index does not have a file format specified in its metadata.
  *
  * Raised during index operations that require knowing the data format
  * (e.g., reading files for a join) when `IndexMetadata.format` is null or
  * empty. This typically indicates a corrupt or manually edited metadata file.
  *
  * {{{
  * // A metadata file with no "format" field will trigger this on join
  * index.join(df, Seq("id")) // throws MissingFormatException
  * }}}
  */
class MissingFormatException
    extends AriadneException("Index doesn't have a specified fileformat")
