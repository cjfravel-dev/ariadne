package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.MetadataMissingOrCorruptException
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{Logger, LogManager}
import scala.io.Source
import java.nio.charset.StandardCharsets
import com.google.gson.Gson

/** Trait providing metadata read/write operations for [[Index]] instances.
  *
  * Manages the lifecycle of [[IndexMetadata]] — reading from and writing to the
  * `metadata.json` file on the Hadoop-compatible filesystem. Metadata is cached
  * in memory after the first load; call [[refreshMetadata]] to force a reload.
  *
  * '''Storage:''' Metadata is persisted as a single JSON file at
  * `{storagePath}/metadata.json`, serialized and deserialized via Gson.
  *
  * '''Error handling:''' If the metadata file is missing or corrupt,
  * [[dev.cjfravel.ariadne.exceptions.MetadataMissingOrCorruptException]] is
  * thrown. Write failures are logged before the exception propagates.
  *
  * '''Thread safety:''' This trait is '''not''' thread-safe. The mutable
  * `_metadata` cache is not synchronized — concurrent calls to
  * [[refreshMetadata]] or [[writeMetadata]] from multiple threads may produce
  * inconsistent state. External synchronization is required for concurrent access.
  *
  * @see [[IndexMetadata]] for the metadata schema and version migration logic
  */
trait IndexMetadataOperations extends AriadneContextUser {
  self: Index =>

  override lazy val logger: Logger = LogManager.getLogger("ariadne")

  /** Hadoop path for the metadata file.
    * @return
    *   the path to `metadata.json` under the index storage directory
    */
  protected def metadataFilePath: Path = new Path(storagePath, "metadata.json")

  /** Checks if metadata exists in the storage location.
    * @return
    *   True if metadata exists, otherwise false.
    */
  protected def metadataExists: Boolean =
    exists(metadataFilePath)

  private var _metadata: IndexMetadata = _

  /** Forces a reload of metadata from the `metadata.json` file on disk.
    *
    * Replaces the in-memory cached metadata with a freshly deserialized copy.
    * This is called automatically by [[metadata]] on first access; callers may
    * invoke it explicitly to pick up external changes.
    *
    * @throws MetadataMissingOrCorruptException
    *   if the metadata file does not exist, cannot be read, or contains
    *   invalid JSON
    */
  def refreshMetadata(): Unit = {
    logger.debug(s"Entering refreshMetadata for index '${name}'")
    _metadata = if (metadataExists) {
      try {
        val inputStream = open(metadataFilePath)
        try {
          val source =
            Source.fromInputStream(inputStream)(StandardCharsets.UTF_8)
          try {
            val jsonString = source.mkString
            IndexMetadata(jsonString)
          } finally {
            source.close()
          }
        } finally {
          inputStream.close()
        }
      } catch {
        case e: Exception =>
          logger.warn(
            s"Failed to read metadata from ${metadataFilePath.toString}: " +
              s"${e.getClass.getSimpleName}: ${e.getMessage}"
          )
          throw new MetadataMissingOrCorruptException(e)
      }
    } else {
      throw new MetadataMissingOrCorruptException()
    }
    logger.warn(s"Read metadata from ${metadataFilePath.toString}")
    logger.debug(s"Completed refreshMetadata for index '${name}'")
  }

  /** Retrieves the stored metadata for the index.
    *
    * @return
    *   IndexMetadata associated with the index.
    * @throws MetadataMissingOrCorruptException
    *   if metadata is missing or cannot be parsed.
    */
  private[ariadne] def metadata: IndexMetadata = {
    if (_metadata == null) {
      refreshMetadata()
    }
    _metadata
  }

  /** Writes metadata to the `metadata.json` file and updates the in-memory cache.
    *
    * Creates the parent directory if it does not exist. The write is ''not''
    * atomic — a crash mid-write may leave a corrupt file that triggers
    * [[dev.cjfravel.ariadne.exceptions.MetadataMissingOrCorruptException]] on
    * the next read.
    *
    * @param metadata
    *   the metadata instance to serialize and persist
    * @throws java.io.IOException
    *   if the write fails (the error is logged before propagating)
    */
  protected def writeMetadata(metadata: IndexMetadata): Unit = {
    require(metadata != null, "metadata must not be null")
    logger.debug(s"Entering writeMetadata for index '${name}'")
    val directoryPath = metadataFilePath.getParent
    if (!exists(directoryPath)) fs.mkdirs(directoryPath)

    val jsonString = new Gson().toJson(metadata)
    var outputStream: org.apache.hadoop.fs.FSDataOutputStream = null
    try {
      outputStream = fs.create(metadataFilePath)
      outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
      outputStream.flush()
    } catch {
      case e: Exception =>
        logger.warn(
          s"Failed to write metadata to ${metadataFilePath.toString}: ${e.getMessage}"
        )
        throw e
    } finally {
      if (outputStream != null) outputStream.close()
    }
    _metadata = metadata // Update in-memory cache
    logger.warn(s"Wrote metadata to ${metadataFilePath.toString}")
    logger.debug(s"Completed writeMetadata for index '${name}'")
  }

  /** Returns the file format of the indexed data (e.g., "parquet", "csv", "json").
    * @return
    *   the format string from the stored metadata
    */
  def format: String = metadata.format

  /** Updates the file format recorded in the stored metadata.
    *
    * @param newFormat
    *   the new format string to persist (e.g., "parquet")
    * @throws IllegalArgumentException
    *   if `newFormat` is null or empty
    */
  private def format_=(newFormat: String): Unit = {
    require(
      newFormat != null && newFormat.nonEmpty,
      "format must not be null or empty"
    )
    val currentMetadata = metadata
    currentMetadata.format = newFormat
    writeMetadata(currentMetadata)
  }
}