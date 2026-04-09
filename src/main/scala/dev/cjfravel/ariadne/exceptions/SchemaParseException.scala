package dev.cjfravel.ariadne.exceptions

/** Thrown when a schema string is found in metadata but cannot be parsed
  * into a valid Spark `StructType`.
  *
  * Raised by `IndexFileOperations.storedSchema` when `DataType.fromJson`
  * cannot deserialize the schema string stored in `IndexMetadata.schema`.
  * This typically indicates a corrupt metadata file or a metadata file
  * written by an incompatible version of Spark.
  *
  * '''Recovery:''' Delete and re-create the index. If the metadata was
  * written by a different Spark version, ensure the same Spark major version
  * is used to read the index.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // A metadata file with schema = "not valid json" triggers this
  * val schema = index.storedSchema // throws SchemaParseException
  * }}}
  */
class SchemaParseException
    extends AriadneException("Schema was found, but could not be parsed")
