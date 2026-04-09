package dev.cjfravel.ariadne.exceptions

/** Thrown when a FileList with the specified name cannot be found on storage.
  *
  * Raised by `FileList.remove` when the Delta table backing the file list does
  * not exist. Also encountered indirectly via `IndexPathUtils.remove` and
  * `IndexCatalog.remove` when cleaning up an index whose file list has already
  * been deleted.
  *
  * '''Recovery:''' This is typically non-fatal during cleanup operations. If
  * encountered during `IndexCatalog.remove`, it is caught and logged
  * internally. If encountered directly, the file list has already been
  * deleted and no further action is needed.
  *
  * '''Thread safety:''' Instances are immutable after construction and safe to
  * share across threads.
  *
  * {{{
  * // Throws FileListNotFoundException if the file list was already removed
  * FileList.remove("[ariadne_index] myIndex")
  * }}}
  *
  * @param name The name of the FileList that was not found
  */
class FileListNotFoundException(name: String)
    extends AriadneException(s"FileList $name was not found")
