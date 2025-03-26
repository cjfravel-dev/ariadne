package dev.cjfravel.ariadne.exceptions

class IndexNotFoundException(name: String)
    extends Exception(s"Index $name was not found")
