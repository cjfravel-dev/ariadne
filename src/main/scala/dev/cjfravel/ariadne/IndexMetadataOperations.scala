package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.MetadataMissingOrCorruptException
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{Logger, LogManager}
import scala.io.Source
import java.nio.charset.StandardCharsets
import com.google.gson.Gson

/** Trait providing metadata operations for Index instances.
  */
trait IndexMetadataOperations extends AriadneContextUser {
  self: Index =>

  override val logger: Logger = LogManager.getLogger("ariadne")

  /** Hadoop path for the metadata file */
  protected def metadataFilePath: Path = new Path(storagePath, "metadata.json")

  /** Checks if metadata exists in the storage location.
    * @return
    *   True if metadata exists, otherwise false.
    */
  protected def metadataExists: Boolean =
    exists(metadataFilePath)

  private var _metadata: IndexMetadata = _

  /** Forces a reload of metadata from disk. */
  def refreshMetadata(): Unit = {
    _metadata = if (metadataExists) {
      try {
        val inputStream = open(metadataFilePath)
        val jsonString =
          Source.fromInputStream(inputStream)(StandardCharsets.UTF_8).mkString
        IndexMetadata(jsonString)
      } catch {
        case _: Exception => throw new MetadataMissingOrCorruptException()
      }
    } else {
      throw new MetadataMissingOrCorruptException()
    }
    logger.warn(s"Read metadata from ${metadataFilePath.toString}")
  }

  /** Retrieves the stored metadata for the index.
    *
    * @return
    *   IndexMetadata associated with the index.
    * @throws MetadataMissingOrCorruptException
    *   if metadata is missing or cannot be parsed.
    */
  protected def metadata: IndexMetadata = {
    if (_metadata == null) {
      refreshMetadata()
    }
    _metadata
  }

  /** Writes metadata to the storage location.
    * @param metadata
    *   The metadata to write.
    */
  protected def writeMetadata(metadata: IndexMetadata): Unit = {
    val directoryPath = metadataFilePath.getParent
    if (!exists(directoryPath)) fs.mkdirs(directoryPath)

    val jsonString = new Gson().toJson(metadata)
    val outputStream = fs.create(metadataFilePath)
    outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
    outputStream.flush()
    outputStream.close()
    _metadata = metadata // Update in-memory cache
    logger.warn(s"Wrote metadata to ${metadataFilePath.toString}")
  }

  /** Returns the format of the stored data. */
  def format: String = metadata.format

  /** Updates the format of the stored data.
    * @param newFormat
    *   The new format to set.
    */
  private def format_=(newFormat: String): Unit = {
    val currentMetadata = metadata
    currentMetadata.format = newFormat
    writeMetadata(currentMetadata)
  }
}