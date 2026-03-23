package dev.cjfravel.ariadne.exceptions

/** Thrown when a distributed lock for an index operation cannot be acquired.
  *
  * This may occur when another process holds the lock and the maximum wait
  * time is exceeded, or when lock acquisition is interrupted.
  *
  * @param message A descriptive error message including index name and lock holder details
  */
class IndexLockException(message: String) extends Exception(message) {
  def this(indexName: String, holderCorrelationId: String, holderOwner: String) =
    this(
      s"Could not acquire lock for index '$indexName'. Currently held by correlationId='$holderCorrelationId', owner='$holderOwner'"
    )
}

object IndexLockException {
  def apply(indexName: String): IndexLockException =
    new IndexLockException(s"Could not acquire lock for index '$indexName' within the configured timeout")
}
