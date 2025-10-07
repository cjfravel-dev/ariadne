package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/** Utility object providing path and utility functions for Index operations.
  */
object IndexPathUtils {
  def storagePath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "indexes")
  }

  def fileListName(name: String): String = s"[ariadne_index] $name"

  def exists(name: String)(implicit sparkSession: SparkSession): Boolean = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    FileList.exists(fileListName(name))(sparkSession) || contextUser.exists(
      new Path(contextUser.storagePath, name)
    )
  }

  def remove(name: String)(implicit sparkSession: SparkSession): Boolean = {
    if (!exists(name)(sparkSession)) {
      throw new IndexNotFoundException(name)
    }

    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    val fileListRemoved = FileList.remove(fileListName(name))(sparkSession)
    contextUser.delete(new Path(contextUser.storagePath, name)) || fileListRemoved
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