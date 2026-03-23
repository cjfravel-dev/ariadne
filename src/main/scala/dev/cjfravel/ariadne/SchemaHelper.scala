package dev.cjfravel.ariadne

import org.apache.spark.sql.types._

/** Utility object for schema-related operations.
  *
  * Provides methods for validating field existence within Spark StructType schemas,
  * including support for nested field paths using dot notation.
  */
object SchemaHelper {

  /** Checks whether a field exists in the given schema, supporting nested paths.
    *
    * Supports dot-separated field paths for nested struct navigation (e.g., "address.city").
    * Note: Does not traverse into ArrayType elements — only direct StructType nesting is supported.
    *
    * @param schema The StructType schema to search
    * @param fieldName The field name or dot-separated path to check
    * @return true if the field exists at the specified path
    */
  def fieldExists(schema: StructType, fieldName: String): Boolean = {
    val parts = fieldName.split("\\.")

    def findField(currentSchema: StructType, path: List[String]): Boolean = {
      path match {
        case Nil => false
        case head :: tail =>
          currentSchema.fields.find(_.name == head) match {
            case Some(field) =>
              field.dataType match {
                case nestedSchema: StructType if tail.nonEmpty =>
                  findField(nestedSchema, tail)
                case _ if tail.isEmpty => true
                case _                 => false
              }
            case None => false
          }
      }
    }

    findField(schema, parts.toList)
  }
}
