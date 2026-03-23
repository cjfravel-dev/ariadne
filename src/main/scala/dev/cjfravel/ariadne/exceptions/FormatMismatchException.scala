package dev.cjfravel.ariadne.exceptions

/** Thrown when the data format provided for an index operation does not match
  * the format stored in the index metadata.
  */
class FormatMismatchException
    extends AriadneException("Format provided does not match stored format")
