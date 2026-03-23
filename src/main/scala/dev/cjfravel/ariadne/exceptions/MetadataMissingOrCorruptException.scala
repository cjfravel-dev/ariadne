package dev.cjfravel.ariadne.exceptions

class MetadataMissingOrCorruptException(cause: Throwable = null)
    extends Exception(
      "Index metadata is missing or corrupt. Delete and re-create the index.",
      cause
    )
