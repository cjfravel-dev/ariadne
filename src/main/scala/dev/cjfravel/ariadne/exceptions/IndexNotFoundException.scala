package dev.cjfravel.ariadne.exceptions

/** Thrown when an index with the specified name does not exist.
  *
  * @param name The name of the index that was not found
  */
class IndexNotFoundException(name: String)
    extends Exception(s"Index $name was not found")
