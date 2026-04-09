package dev.cjfravel.ariadne.exceptions

/** Thrown when a distributed lock for an index operation cannot be acquired.
  *
  * This may occur when another process holds the lock and the maximum wait
  * time (`lockMaxWait`) is exceeded, when lock acquisition is interrupted,
  * or when a corrupt lock file cannot be recovered after multiple attempts.
  *
  * Raised by [[dev.cjfravel.ariadne.IndexLock.acquire]] and propagated through
  * `Index.update`, `Index.compact`, `Index.vacuum`, and `Index.deleteFiles`.
  *
  * '''Recovery:''' Retry the operation after a delay, increase the
  * `lockMaxWait` configuration, or check for stale/orphaned lock files in
  * the index storage path. Lock files are stored as `.update.lock` and
  * `.filelist.lock` under the index directory.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads. Note that lock contention is inherently a
  * concurrency concern — `IndexLock.acquire` is not reentrant.
  *
  * {{{
  * try {
  *   index.update("s3a://bucket/data/")
  * } catch {
  *   case e: IndexLockException =>
  *     // Another process holds the lock or lock file is corrupt
  *     logger.error(s"Lock contention: \${e.getMessage}")
  * }
  * }}}
  *
  * @param message A descriptive error message including index name and lock holder details
  */
class IndexLockException(message: String) extends AriadneException(message) {

  /** Creates an `IndexLockException` with details about the current lock holder.
    *
    * @param indexName            the name of the index whose lock could not be acquired
    * @param holderCorrelationId  the correlation ID of the process holding the lock
    * @param holderOwner          the owner identifier of the process holding the lock
    */
  def this(indexName: String, holderCorrelationId: String, holderOwner: String) =
    this(
      s"Could not acquire lock for index '$indexName'. Currently held by correlationId='$holderCorrelationId', owner='$holderOwner'"
    )
}

/** Factory for [[IndexLockException]] instances.
  *
  * Provides a convenient `apply` method to create timeout-based lock exceptions
  * without specifying holder details.
  */
object IndexLockException {

  /** Creates an [[IndexLockException]] for a lock acquisition timeout.
    *
    * @param indexName the name of the index whose lock could not be acquired
    * @return a new `IndexLockException` with a timeout message
    */
  def apply(indexName: String): IndexLockException =
    new IndexLockException(s"Could not acquire lock for index '$indexName' within the configured timeout")
}
