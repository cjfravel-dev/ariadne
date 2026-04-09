package dev.cjfravel.ariadne.exceptions

/** Thrown when an index with the specified name does not exist in storage.
  *
  * Raised by `IndexCatalog.get`, `IndexCatalog.remove`, `IndexCatalog.describe`,
  * `IndexPathUtils.remove`, and `Index.remove` when the requested index name
  * cannot be found in the configured `spark.ariadne.storagePath`.
  *
  * '''Recovery:''' Verify the index name is correct and that
  * `spark.ariadne.storagePath` points to the expected storage location.
  * Use `IndexCatalog.list()` to see available indexes.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // Throws IndexNotFoundException if "missing" does not exist
  * val index = IndexCatalog.get("missing")
  * }}}
  *
  * @param name The name of the index that was not found
  */
class IndexNotFoundException(name: String)
    extends AriadneException(s"Index $name was not found")
