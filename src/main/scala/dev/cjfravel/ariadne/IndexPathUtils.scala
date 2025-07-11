package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.hadoop.fs.Path

/** Utility object providing path and utility functions for Index operations.
  */
object IndexPathUtils extends AriadneContextUser {
  override def storagePath: Path = new Path(super.storagePath, "indexes")

  def fileListName(name: String): String = s"[ariadne_index] $name"

  def exists(name: String): Boolean =
    FileList.exists(fileListName(name)) || super.exists(
      new Path(super.storagePath, name)
    )

  def remove(name: String): Boolean = {
    if (!exists(name)) {
      throw new IndexNotFoundException(name)
    }

    val fileListRemoved = FileList.remove(fileListName(name))
    delete(new Path(super.storagePath, name)) || fileListRemoved
  }

  /** Cleans a filename for safe storage by replacing special characters.
    *
    * @param fileName The filename to clean
    * @return The cleaned filename
    */
  def cleanFileName(fileName: String): String = {
    fileName
      .replaceAll("[^a-zA-Z0-9]", "_")
      .replaceAll("__+", "_")
      .replaceAll("^_|_$", "")
  }
}