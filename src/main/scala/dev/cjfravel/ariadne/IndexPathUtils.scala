package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

/** Utility object providing path resolution and lifecycle operations for
  * indexes.
  *
  * All paths are resolved relative to the configured
  * `spark.ariadne.storagePath`. This object is stateless; filesystem access is
  * performed through temporary [[AriadneContextUser]] instances created from
  * the implicit `SparkSession`.
  */
object IndexPathUtils {

  private val logger = LogManager.getLogger("ariadne")

  /** Returns the base storage path for indexes under the configured Ariadne
    * storage path.
    *
    * @param sparkSession
    *   the implicit SparkSession providing configuration
    * @return
    *   the Hadoop Path to the `indexes` directory
    */
  def storagePath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "indexes")
  }

  /** Validates that an index name is safe to use as a path component.
    *
    * Rejects names that are null, blank, contain path separators (`/`, `\`),
    * contain the parent-directory segment `..`, contain null bytes, or begin
    * with `.` (to avoid colliding with hidden files). Valid index names are
    * used directly as subdirectory names under the Ariadne storage root, so
    * unchecked input would permit path traversal (e.g., `../foo`) or
    * collisions with internal control files.
    *
    * @param name the index name to validate
    * @throws IllegalArgumentException if the name is null, blank, or contains unsafe characters
    */
  def validateIndexName(name: String): Unit = {
    require(name != null && name.trim.nonEmpty, "index name must not be null or blank")
    require(
      !name.contains('/') && !name.contains('\\') && !name.contains('\u0000'),
      s"index name must not contain path separators or null bytes: '$name'"
    )
    require(
      !name.startsWith("."),
      s"index name must not start with '.': '$name'"
    )
    require(
      name != ".." && !name.split("[/\\\\]").contains(".."),
      s"index name must not contain parent-directory segments: '$name'"
    )
  }

  /** Returns the file list name for a given index name.
    *
    * @param name
    *   The index name
    * @return
    *   The prefixed file list identifier
    * @throws IllegalArgumentException
    *   if name is null, blank, or contains unsafe path characters
    */
  def fileListName(name: String): String = {
    validateIndexName(name)
    s"[ariadne_index] $name"
  }

  /** Checks whether an index with the given name exists.
    *
    * An index is considered to exist if either its file list entry or its
    * storage directory is present.
    *
    * @note This check is subject to a TOCTOU (time-of-check/time-of-use) race
    *       condition — the index may be created or removed between this call
    *       and a subsequent operation. Callers must not rely on this result
    *       for correctness in concurrent environments; use an external lock
    *       or perform the dependent operation defensively.
    *
    * @param name
    *   The index name to check
    * @param sparkSession
    *   The implicit SparkSession
    * @return
    *   true if the index exists
    * @throws IllegalArgumentException
    *   if name is null, blank, or contains unsafe path characters
    */
  def exists(name: String)(implicit sparkSession: SparkSession): Boolean = {
    validateIndexName(name)
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    FileList.exists(fileListName(name))(sparkSession) || contextUser.exists(
      new Path(storagePath(sparkSession), name)
    )
  }

  /** Removes an index by deleting its file list entry and storage directory.
    *
    * Both the file list Delta table and the index storage directory are
    * removed. The method returns `true` if at least one resource was
    * successfully deleted.
    *
    * @note The [[exists]] guard is subject to a TOCTOU (time-of-check/time-of-use)
    *       race — another process may remove the index between the existence
    *       check and the actual deletion, or create a new index with the same
    *       name concurrently. External locking is required to prevent this in
    *       multi-process environments.
    *
    * @param name
    *   the index name to remove
    * @param sparkSession
    *   the implicit SparkSession
    * @return
    *   true if any resources were successfully removed
    * @throws IndexNotFoundException
    *   if the index does not exist
    * @throws IllegalArgumentException
    *   if name is null, blank, or contains unsafe path characters
    */
  def remove(name: String)(implicit sparkSession: SparkSession): Boolean = {
    validateIndexName(name)
    if (!exists(name)(sparkSession)) {
      throw new IndexNotFoundException(name)
    }

    logger.warn(s"Removing index '${name}' (file list and storage directory)")
    val startTime = System.currentTimeMillis()
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    val fileListRemoved = try {
      FileList.remove(fileListName(name))(sparkSession)
    } catch {
      case e: Exception =>
        logger.warn(s"FileList removal failed for index '$name' (continuing with directory deletion): ${e.getMessage}")
        false
    }
    val result = contextUser.delete(
      new Path(storagePath(sparkSession), name)
    ) || fileListRemoved
    val elapsed = System.currentTimeMillis() - startTime
    logger.warn(s"Successfully removed index '$name' in ${elapsed}ms")
    result
  }

  /** Cleans a filename for safe storage by replacing non-alphanumeric
    * characters with underscores, collapsing consecutive underscores, and
    * trimming leading/trailing underscores.
    *
    * @param fileName
    *   the raw filename to clean; must not be null
    * @return
    *   the sanitized filename, or an empty string if input is empty
    * @throws IllegalArgumentException
    *   if `fileName` is null
    */
  def cleanFileName(fileName: String): String = {
    if (fileName == null) {
      throw new IllegalArgumentException(
        "fileName must not be null"
      )
    }
    if (fileName.isEmpty) {
      ""
    } else {
      fileName
        .replaceAll("[^a-zA-Z0-9]", "_")
        .replaceAll("_+", "_")
        .replaceAll("^_+|_+$", "")
    }
  }

  /** Returns a temporary directory path for query staging operations.
    *
    * The `_temp` directory is located under the Ariadne storage path and is
    * used for transient data during query execution.
    *
    * @param sparkSession
    *   the implicit SparkSession
    * @return
    *   Hadoop Path to the `_temp` directory
    */
  def tempPath(implicit sparkSession: SparkSession): Path = {
    val contextUser = new AriadneContextUser {
      implicit def spark: SparkSession = sparkSession
    }
    new Path(contextUser.storagePath, "_temp")
  }
}
