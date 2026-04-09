package dev.cjfravel.ariadne.exceptions

/** Thrown when the index metadata file (`metadata.json`) is missing, empty,
  * or cannot be parsed into a valid [[dev.cjfravel.ariadne.IndexMetadata]].
  *
  * Raised by `IndexMetadataOperations.refreshMetadata` when the file does not
  * exist or contains invalid JSON, and by `IndexMetadata.apply` when the JSON
  * string is null, empty, or deserializes to null. Recovery typically requires
  * deleting the corrupt index and re-creating it.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * try {
  *   val index = Index("corruptIndex")
  *   index.metadata // triggers read
  * } catch {
  *   case e: MetadataMissingOrCorruptException =>
  *     // Delete and re-create the index
  *     IndexCatalog.remove("corruptIndex")
  * }
  * }}}
  *
  * @param cause The underlying exception that caused the parse failure, or null if the file is missing
  */
class MetadataMissingOrCorruptException(cause: Throwable = null)
    extends AriadneException(
      "Index metadata is missing or corrupt. Delete and re-create the index.",
      cause
    )
