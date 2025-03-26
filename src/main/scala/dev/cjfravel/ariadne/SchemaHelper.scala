package dev.cjfravel.ariadne

import org.apache.spark.sql.types._

object SchemaHelper {
  import org.apache.spark.sql.types._

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
