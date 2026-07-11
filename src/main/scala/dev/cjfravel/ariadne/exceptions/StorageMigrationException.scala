package dev.cjfravel.ariadne.exceptions

/**
 * Thrown when a persisted index cannot be migrated safely to the current storage format.
 *
 * Operations abort without advancing `storage_format_version`. Correct the reported collision/corruption and retry;
 * idempotent migration detection will resume from the persisted version.
 *
 * @param message
 *   description of the failed migration invariant
 * @param cause
 *   optional underlying filesystem or Delta exception
 */
class StorageMigrationException(message: String, cause: Throwable = null) extends AriadneException(message, cause)
