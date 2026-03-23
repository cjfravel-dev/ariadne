package dev.cjfravel.ariadne.exceptions

/** Thrown when an index with the specified name does not exist in storage.
  *
  * Raised by `IndexCatalog.get`, `IndexCatalog.remove`, `IndexCatalog.describe`,
  * and `IndexPathUtils.remove` when the requested index name cannot be found
  * in the configured `spark.ariadne.storagePath`.
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
