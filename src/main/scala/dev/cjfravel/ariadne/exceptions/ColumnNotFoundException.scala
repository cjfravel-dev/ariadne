package dev.cjfravel.ariadne.exceptions

class ColumnNotFoundException(column: String) extends Exception(s"Column $column was not found in the dataframe")