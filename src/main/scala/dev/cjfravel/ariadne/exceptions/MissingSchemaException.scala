package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema field is missing from the index metadata.
  *
  * Raised by `IndexFileOperations.storedSchema` when `IndexMetadata.schema`
  * is null or empty. This typically indicates a corrupt or manually edited
  * metadata file, since `Index.apply` always writes a schema during creation.
  *
  * '''Recovery:''' Delete and re-create the index, or restore the metadata
  * file from a backup.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // A metadata file with no "schema" field will trigger this
  * val schema = index.storedSchema // throws MissingSchemaException
  * }}}
  */
class MissingSchemaException extends AriadneException("Schema missing from metadata")
