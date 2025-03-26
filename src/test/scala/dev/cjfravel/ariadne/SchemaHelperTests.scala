package dev.cjfravel.ariadne

import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

class SchemaHelperTests extends AnyFunSuite {
  val schema = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField(
        "address",
        StructType(
          Seq(
            StructField("street", StringType, true),
            StructField("city", StringType, true)
          )
        ),
        true
      )
    )
  )

  test("schema contains") {
    assert(SchemaHelper.fieldExists(schema, "id") === true)
    assert(SchemaHelper.fieldExists(schema, "name") === true)
    assert(SchemaHelper.fieldExists(schema, "address.city") === true)
    assert(SchemaHelper.fieldExists(schema, "address.zip") === false)
    assert(SchemaHelper.fieldExists(schema, "phone") === false)
  }

}
