package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/** Utility object providing path and utility functions for Index operations.
  */
object IndexPathUtils {

  /** Returns the base storage path for indexes under the configured Ariadne storage path.
    *
    * @param sparkSession The implicit SparkSession providing configuration
    * @return The Hadoop Path to the indexes directory
    */
  def storagePath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "indexes")
  }

  /** Returns the file list name for a given index name.
    *
    * @param name The index name
    * @return The prefixed file list identifier
    */
  def fileListName(name: String): String = s"[ariadne_index] $name"

  /** Checks whether an index with the given name exists.
    *
    * An index is considered to exist if either its file list entry or its
    * storage directory is present.
    *
    * @param name The index name to check
    * @param sparkSession The implicit SparkSession
    * @return true if the index exists
    */
  def exists(name: String)(implicit sparkSession: SparkSession): Boolean = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    FileList.exists(fileListName(name))(sparkSession) || contextUser.exists(
      new Path(contextUser.storagePath, name)
    )
  }

  /** Removes an index by deleting its file list entry and storage directory.
    *
    * @param name The index name to remove
    * @param sparkSession The implicit SparkSession
    * @return true if any resources were successfully removed
    * @throws IndexNotFoundException if the index does not exist
    */
  def remove(name: String)(implicit sparkSession: SparkSession): Boolean = {
    if (!exists(name)(sparkSession)) {
      throw new IndexNotFoundException(name)
    }

    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    val fileListRemoved = FileList.remove(fileListName(name))(sparkSession)
    contextUser.delete(
      new Path(contextUser.storagePath, name)
    ) || fileListRemoved
  }

  /** Cleans a filename for safe storage by replacing special characters.
    *
    * @param fileName
    *   The filename to clean
    * @return
    *   The cleaned filename
    */
  def cleanFileName(fileName: String): String = {
    if (fileName == null || fileName.isEmpty) return ""
    fileName
      .replaceAll("[^a-zA-Z0-9]", "_")
      .replaceAll("_+", "_")
      .replaceAll("^_+|_+$", "")
  }

  /** Creates a temp path for query staging operations.
    *
    * @param sparkSession
    *   The SparkSession
    * @return
    *   Path to temp directory
    */
  def tempPath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "_temp")
  }
}
