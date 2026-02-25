package dev.cjfravel.ariadne.exceptions

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
