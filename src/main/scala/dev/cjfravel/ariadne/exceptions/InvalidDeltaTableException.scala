package dev.cjfravel.ariadne.exceptions

import org.apache.hadoop.fs.Path

/**
 * Thrown when a path Ariadne expects to hold a Delta table exists and contains data, but does not contain readable
 * Delta metadata.
 *
 * This normally means one of:
 *   - a previous write was interrupted, leaving a `_delta_log` with no committed version;
 *   - the Delta log was truncated, vacuumed away, or partially deleted;
 *   - a path collision — the configured `spark.ariadne.storagePath` points at a directory that already holds unrelated
 *     data.
 *
 * Ariadne does '''not''' attempt to repair or remove the directory. It cannot distinguish an abandoned partial write of
 * its own from data that belongs to someone else, so deleting would risk destroying data that Ariadne did not create.
 * The path is reported and left exactly as it was found.
 *
 * '''Recovery:''' Inspect the reported path. If the contents are a known-abandoned Ariadne write, delete the directory
 * so it can be recreated on the next write. If the contents are unrelated data, point the index at a different
 * `spark.ariadne.storagePath` instead. Ariadne resumes normally once the path is either a valid Delta table, an empty
 * directory, or absent.
 *
 * '''Thread safety:''' Instances are immutable after construction and safe to share across threads.
 *
 * {{{
 * try {
 *   index.update()
 * } catch {
 *   case e: InvalidDeltaTableException =>
 *     // Do not delete blindly — check what is actually at the path first
 *     logger.error(e.getMessage)
 * }
 * }}}
 *
 * @param message
 *   A descriptive error message naming the offending path
 */
class InvalidDeltaTableException(message: String) extends AriadneException(message)

/**
 * Factory for [[InvalidDeltaTableException]] instances.
 */
object InvalidDeltaTableException {

  /**
   * Creates an [[InvalidDeltaTableException]] naming the path that could not be read as a Delta table.
   *
   * @param path
   *   the path that exists and is non-empty but holds no readable Delta metadata
   * @return
   *   a new `InvalidDeltaTableException`
   */
  def apply(path: Path): InvalidDeltaTableException =
    new InvalidDeltaTableException(
      s"Path '$path' exists and is not empty, but does not contain a readable Delta table. Ariadne will not delete " +
        s"it, because it cannot tell an abandoned partial write from data it does not own. Inspect the path: if it " +
        s"is a leftover Ariadne write, remove the directory so it can be recreated; if it holds unrelated data, " +
        s"configure a different 'spark.ariadne.storagePath'.")
}
