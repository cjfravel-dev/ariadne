package dev.cjfravel.ariadne.exceptions

/** Base exception class for all Ariadne library errors.
  *
  * Extends `RuntimeException` following Scala conventions for unchecked exceptions.
  * All domain-specific exceptions in the Ariadne library extend this class, enabling
  * callers to catch any Ariadne error with a single handler if desired.
  *
  * {{{
  * try {
  *   index.update("s3a://bucket/data/")
  * } catch {
  *   case e: AriadneException =>
  *     // handles any Ariadne-specific error
  *     logger.error(s"Ariadne operation failed: \${e.getMessage}", e)
  * }
  * }}}
  *
  * @param message descriptive error message
  * @param cause   optional underlying cause of the error
  */
class AriadneException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
