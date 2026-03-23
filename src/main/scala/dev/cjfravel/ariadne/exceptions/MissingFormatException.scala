package dev.cjfravel.ariadne.exceptions

/** Thrown when an index does not have a file format specified in its metadata.
  */
class MissingFormatException
    extends Exception("Index doesn't have a specified fileformat")
