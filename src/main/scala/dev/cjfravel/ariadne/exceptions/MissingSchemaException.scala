package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema field is missing from the index metadata.
  */
class MissingSchemaException extends Exception("Schema missing from metadata")
