package dev.cjfravel.ariadne.exceptions

/** Thrown when no existing metadata is found and no schema was provided
  * to create a new index.
  */
class SchemaNotProvidedException
    extends AriadneException("No existing metadata found, schema must be provided")
