package dev.cjfravel.ariadne.exceptions

/** Thrown when a schema string is found in metadata but cannot be parsed
  * into a valid Spark StructType.
  */
class SchemaParseException
    extends AriadneException("Schema was found, but could not be parsed")
