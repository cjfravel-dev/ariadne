package dev.cjfravel.ariadne.exceptions

/** Thrown when no existing metadata is found and no schema was provided
  * to create a new index.
  *
  * Raised by `Index.apply` when the index does not yet exist on storage and
  * the caller did not supply a schema. A schema is required for first-time
  * index creation so that column validation can be performed.
  *
  * '''Recovery:''' Provide a schema when creating a new index, e.g.,
  * `Index("myIndex", schema, "parquet")`.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // Index "newIndex" does not exist yet — schema is required
  * Index("newIndex")                       // throws SchemaNotProvidedException
  * Index("newIndex", schema, "parquet")    // OK
  * }}}
  */
class SchemaNotProvidedException
    extends AriadneException("No existing metadata found, schema must be provided")
