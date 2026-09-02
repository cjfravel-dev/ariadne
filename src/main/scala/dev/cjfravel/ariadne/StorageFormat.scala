package dev.cjfravel.ariadne

/**
 * Persisted metadata and physical storage format versions supported by Ariadne.
 *
 * Metadata and storage versions evolve independently. Metadata version 10 represents the fully normalized additive
 * metadata schema. Storage versions describe the complete persisted index layout from the alpha37 compatibility floor
 * through the current canonical layout.
 *
 * Each storage version constant names the layout change it introduces, so migration preflight can gate each step on its
 * own version rather than on [[CurrentStorageVersion]]. Gating on [[CurrentStorageVersion]] would re-run every earlier
 * migration whenever the current version is raised.
 */
private[ariadne] object StorageFormat {
  val CurrentMetadataVersion: Int = 10
  val Alpha37StorageVersion: Int = 1
  val FileSizeStorageVersion: Int = 2
  val ExplodedFieldStorageVersion: Int = 3
  val AutoBloomBackfillStorageVersion: Int = 4
  val CurrentStorageVersion: Int = 4
}
