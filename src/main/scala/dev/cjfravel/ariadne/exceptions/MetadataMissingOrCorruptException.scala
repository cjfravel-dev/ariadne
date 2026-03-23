package dev.cjfravel.ariadne.exceptions

/** Thrown when the index metadata file is missing, empty, or cannot be parsed.
  *
  * @param cause The underlying exception that caused the parse failure, or null if the file is missing
  */
class MetadataMissingOrCorruptException(cause: Throwable = null)
    extends Exception(
      "Index metadata is missing or corrupt. Delete and re-create the index.",
      cause
    )
