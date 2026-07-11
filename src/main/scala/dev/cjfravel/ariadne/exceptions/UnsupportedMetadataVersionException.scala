package dev.cjfravel.ariadne.exceptions

/**
 * Thrown when an index declares a metadata schema newer than this Ariadne build supports.
 *
 * Upgrade Ariadne before opening the index so unknown metadata semantics are not discarded or misinterpreted.
 *
 * @param found
 *   metadata version declared by the index
 * @param supported
 *   newest metadata version supported by this build
 */
class UnsupportedMetadataVersionException(found: Int, supported: Int)
    extends StorageMigrationException(
      s"Index metadata version $found is newer than supported version $supported. Upgrade Ariadne.")
