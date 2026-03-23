package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema field is missing from the index metadata.
  *
  * Raised during index operations that require the DataFrame schema
  * (e.g., reading indexed files) when `IndexMetadata.schema` is null or
  * empty. This typically indicates a corrupt or manually edited metadata file.
  *
  * {{{
  * // A metadata file with no "schema" field will trigger this on join
  * index.join(df, Seq("id")) // throws MissingSchemaException
  * }}}
  */
class MissingSchemaException extends AriadneException("Schema missing from metadata")
