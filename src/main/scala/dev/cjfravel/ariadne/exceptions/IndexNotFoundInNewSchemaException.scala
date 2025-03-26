package dev.cjfravel.ariadne.exceptions

class IndexNotFoundInNewSchemaException(col: String)
    extends Exception(s"Index $col was not found in new schema")
