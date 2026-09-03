package dev.cjfravel.ariadne

import org.apache.spark.sql.types._

/**
 * Utility object for Spark schema introspection.
 *
 * Provides methods for validating field existence within [[org.apache.spark.sql.types.StructType]] schemas, including
 * support for nested field paths using dot notation. Used throughout Ariadne to validate that indexed columns exist in
 * the user-supplied schema before persisting metadata.
 *
 * '''Thread safety:''' All methods are pure functions with no mutable state; safe to call concurrently from any thread.
 */
object SchemaHelper {

  /**
   * Checks whether a field exists in the given schema, supporting nested paths.
   *
   * Supports dot-separated field paths for nested struct navigation (e.g., `"address.city"` looks for a `city` field
   * inside an `address` struct). Only direct `StructType` nesting is traversed — `ArrayType` elements are ''not''
   * descended into.
   *
   * @param schema
   *   The root `StructType` schema to search. Must not be null.
   * @param fieldName
   *   A field name or dot-separated path (e.g., `"user.profile.id"`). Must not be null or blank.
   * @return
   *   `true` if the field exists at the specified path, `false` otherwise
   * @throws IllegalArgumentException
   *   if schema is null or fieldName is null/blank
   */
  def fieldExists(schema: StructType, fieldName: String): Boolean = {
    require(schema != null, "schema must not be null")
    require(fieldName != null && fieldName.trim.nonEmpty, "fieldName must not be null or blank")
    val parts = fieldName.split("\\.")

    def findField(currentSchema: StructType, path: List[String]): Boolean =
      path match {
        case Nil => false
        case head :: tail =>
          currentSchema.fields.find(_.name == head) match {
            case Some(field) =>
              field.dataType match {
                case nestedSchema: StructType if tail.nonEmpty =>
                  findField(nestedSchema, tail)
                case _ if tail.isEmpty => true
                case _ => false
              }
            case None => false
          }
      }

    findField(schema, parts.toList)
  }

  /**
   * Returns the data type of a top-level field in the schema.
   *
   * @param schema
   *   The root `StructType` schema to search. Must not be null.
   * @param fieldName
   *   The top-level field name to look up. Must not be null or blank.
   * @return
   *   `Some(dataType)` if the field exists, `None` otherwise
   * @throws IllegalArgumentException
   *   if schema is null or fieldName is null/blank
   */
  def fieldType(schema: StructType, fieldName: String): Option[DataType] = {
    require(schema != null, "schema must not be null")
    require(fieldName != null && fieldName.trim.nonEmpty, "fieldName must not be null or blank")
    schema.fields.find(_.name == fieldName).map(_.dataType)
  }

  /**
   * Resolves a dotted field path to its data type, descending through structs and arrays of structs.
   *
   * This mirrors Spark's dotted column access. `col("users.id")` on an `array<struct<id: long>>` column selects the
   * `id` field of every element, so array levels are traversed transparently rather than terminating the path. The type
   * returned is the '''element''' type at the end of the path (`LongType` in that example), not the array that Spark
   * would wrap it in, because callers care about the type of the values themselves.
   *
   * Used to establish the type behind an exploded-field index alias, which exists only as a mapping in metadata and
   * never as a top-level column on the source DataFrame — [[fieldType]] cannot see it.
   *
   * @param schema
   *   The root `StructType` schema to search. Must not be null.
   * @param path
   *   The dotted path, e.g. `"users.id"`. Must not be null or blank.
   * @return
   *   `Some(dataType)` if the whole path resolves, `None` if any segment is missing or a non-descendable type is
   *   encountered before the path is consumed
   * @throws IllegalArgumentException
   *   if schema is null or path is null/blank
   */
  def nestedFieldType(schema: StructType, path: String): Option[DataType] = {
    require(schema != null, "schema must not be null")
    require(path != null && path.trim.nonEmpty, "path must not be null or blank")

    def descend(dataType: DataType, remaining: List[String]): Option[DataType] =
      remaining match {
        case Nil => Some(dataType)
        case head :: tail =>
          dataType match {
            case struct: StructType =>
              struct.fields.find(_.name == head).flatMap(field => descend(field.dataType, tail))
            case ArrayType(elementType, _) => descend(elementType, remaining)
            case _ => None
          }
      }

    descend(schema, path.split("\\.").toList)
  }

  /**
   * Reports whether a data type is, or transitively contains, a [[org.apache.spark.sql.types.BinaryType]].
   *
   * Spark materializes `BinaryType` as a JVM `Array[Byte]`, whose `toString` is identity-based (`[B@1b6d3586`) rather
   * than value-based. Any Ariadne feature that canonicalizes a value by calling `toString` therefore produces a
   * different token for every occurrence of the same bytes, including two references to the same content. Callers use
   * this to reject such columns up front instead of building a structure that can never match.
   *
   * Arrays, maps (both key and value types) and structs are descended into, so a struct holding a binary field is
   * reported just as a bare binary column is.
   *
   * @param dataType
   *   The data type to inspect. Must not be null.
   * @return
   *   `true` if the type is `BinaryType` or contains one at any depth, `false` otherwise
   * @throws IllegalArgumentException
   *   if dataType is null
   */
  def containsBinaryType(dataType: DataType): Boolean = {
    require(dataType != null, "dataType must not be null")
    dataType match {
      case BinaryType => true
      case ArrayType(elementType, _) => containsBinaryType(elementType)
      case MapType(keyType, valueType, _) => containsBinaryType(keyType) || containsBinaryType(valueType)
      case struct: StructType => struct.fields.exists(field => containsBinaryType(field.dataType))
      case _ => false
    }
  }
}
