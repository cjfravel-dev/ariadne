package dev.cjfravel.ariadne.exceptions

/** Thrown when a FileList with the specified name cannot be found on storage.
  *
  * Raised by `FileList.remove` when the Delta table backing the file list does
  * not exist. Also encountered indirectly via `IndexPathUtils.remove` and
  * `IndexCatalog.remove` when cleaning up an index whose file list has already
  * been deleted.
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
