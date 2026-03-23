package dev.cjfravel.ariadne.exceptions

/** Thrown when a FileList with the specified name cannot be found.
  *
  * @param name The name of the FileList that was not found
  */
class FileListNotFoundException(name: String)
    extends Exception(s"FileList $name was not found")
