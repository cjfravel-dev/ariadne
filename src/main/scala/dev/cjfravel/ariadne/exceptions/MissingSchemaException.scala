package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema field is missing from the index metadata.
  */
class MissingSchemaException extends AriadneException("Schema missing from metadata")
