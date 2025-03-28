package dev.cjfravel.ariadne.exceptions

class FileListNotFoundException(name: String)
    extends Exception(s"FileList $name was not found")
