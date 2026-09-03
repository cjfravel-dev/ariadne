package dev.cjfravel.ariadne

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for [[SchemaHelper]] verifying field lookup in flat and nested schemas.
 */
class SchemaHelperTests extends AnyFunSuite {
  val schema =
    StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField(
          "address",
          StructType(Seq(StructField("street", StringType, true), StructField("city", StringType, true))),
          true)))

  test("schema contains") {
    assert(SchemaHelper.fieldExists(schema, "id") === true)
    assert(SchemaHelper.fieldExists(schema, "name") === true)
    assert(SchemaHelper.fieldExists(schema, "address.city") === true)
    assert(SchemaHelper.fieldExists(schema, "address.zip") === false)
    assert(SchemaHelper.fieldExists(schema, "phone") === false)
  }

  test("containsBinaryType detects binary at every depth") {
    assert(SchemaHelper.containsBinaryType(BinaryType))
    assert(SchemaHelper.containsBinaryType(ArrayType(BinaryType)))
    assert(SchemaHelper.containsBinaryType(MapType(StringType, BinaryType)))
    assert(SchemaHelper.containsBinaryType(MapType(BinaryType, StringType)))
    assert(SchemaHelper.containsBinaryType(StructType(Seq(StructField("bytes", BinaryType)))))
    assert(SchemaHelper.containsBinaryType(ArrayType(StructType(Seq(StructField("bytes", BinaryType))))))
  }

  test("containsBinaryType passes types that canonicalize by value") {
    assert(!SchemaHelper.containsBinaryType(StringType))
    assert(!SchemaHelper.containsBinaryType(IntegerType))
    assert(!SchemaHelper.containsBinaryType(TimestampType))
    assert(!SchemaHelper.containsBinaryType(ArrayType(StringType)))
    assert(!SchemaHelper.containsBinaryType(StructType(Seq(StructField("id", LongType)))))
  }

}
