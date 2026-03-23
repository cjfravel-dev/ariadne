package dev.cjfravel.ariadne.exceptions

/** Thrown when the schema provided for an index operation does not match
  * the schema stored in the existing index metadata.
  */
class SchemaMismatchException
    extends AriadneException("Schema provided does not match stored schema")
