package dev.cjfravel.ariadne.exceptions

/** Thrown when a schema string is found in metadata but cannot be parsed
  * into a valid Spark `StructType`.
  *
  * Raised during `Index.apply` when `IndexMetadata.schema` contains a value
  * that `DataType.fromJson` cannot deserialize. This typically indicates a
  * corrupt metadata file or a metadata file written by an incompatible
  * version of Spark.
  *
  * {{{
  * // A metadata file with schema = "not valid json" triggers this
  * val index = Index("corruptSchemaIndex") // throws SchemaParseException
  * }}}
  */
class SchemaParseException
    extends AriadneException("Schema was found, but could not be parsed")
