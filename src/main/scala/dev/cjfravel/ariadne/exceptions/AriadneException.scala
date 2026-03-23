package dev.cjfravel.ariadne.exceptions

/** Base exception class for all Ariadne library errors.
  *
  * Extends `RuntimeException` following Scala conventions for unchecked exceptions.
  * All domain-specific exceptions in the Ariadne library extend this class, enabling
  * callers to catch any Ariadne error with a single handler if desired.
  *
  * @param message descriptive error message
  * @param cause   optional underlying cause of the error
  */
class AriadneException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
