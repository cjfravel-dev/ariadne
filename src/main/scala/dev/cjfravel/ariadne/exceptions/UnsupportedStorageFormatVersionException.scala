package dev.cjfravel.ariadne.exceptions

/**
 * Thrown when an index declares a storage format newer than this Ariadne build supports.
 *
 * Upgrade Ariadne to a release supporting the declared version. Never rebuild or downgrade the metadata in place,
 * because the newer physical layout may not be understood safely.
 *
 * @param found
 *   storage format version declared by the index
 * @param supported
 *   newest storage format supported by this build
 */
class UnsupportedStorageFormatVersionException(found: Int, supported: Int)
    extends StorageMigrationException(
      s"Index storage format version $found is newer than supported version $supported. Upgrade Ariadne.")
