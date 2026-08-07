package dev.cjfravel.ariadne

import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Exhaustive mutual-exclusivity tests across index types.
 *
 * A column may carry exactly one index type. Each `add*Index` method enforced this with its own hand-written list of
 * checks, so a missing entry made the rule depend on registration order: one order threw and the reverse silently
 * accepted a configuration that produces wrong results at query time.
 *
 * These tests assert the matrix is symmetric by walking every ordered pair, so a new index type cannot reintroduce a
 * one-directional gap without failing here.
 */
class IndexTypeExclusivityTests extends SparkTests with Matchers {

  val exclusivitySchema =
    StructType(
      Seq(
        StructField("Id", IntegerType, nullable = false),
        StructField("Value", DoubleType, nullable = false),
        StructField("UpdatedAt", TimestampType, nullable = true),
        StructField(
          "items",
          ArrayType(StructType(Seq(StructField("id", IntegerType, nullable = true)))),
          nullable = true)))

  /** Registers one index type on the shared column `Id`. */
  private case class IndexKind(label: String, register: Index => Unit)

  private val indexKinds =
    Seq(
      IndexKind("regular", _.addIndex("Id")),
      IndexKind("computed", _.addComputedIndex("Id", "1")),
      IndexKind("exploded", _.addExplodedFieldIndex("items", "id", "Id")),
      IndexKind("bloom", _.addBloomIndex("Id")),
      IndexKind("temporal", _.addTemporalIndex("Id", "UpdatedAt")),
      IndexKind("range", _.addRangeIndex("Id")))

  test("every pair of index types on the same column is mutually exclusive in both orders") {
    val gaps =
      for {
        first <- indexKinds
        second <- indexKinds
        if first.label != second.label
        gap <- {
          val index = Index(s"exclusivity_${first.label}_${second.label}", exclusivitySchema, "parquet")
          first.register(index)

          try {
            second.register(index)
            // Registering the second type must not be accepted; if it is, the column now carries two
            // index types and queries silently return a wrong result set.
            Some(s"${first.label} -> ${second.label}")
          } catch {
            case _: IllegalArgumentException => None
          }
        }
      } yield gap

    withClue(s"index type pairs that were wrongly accepted: ${gaps.mkString(", ")}\n") {
      gaps shouldBe empty
    }
  }

  test("re-adding the same index type stays idempotent") {
    // The exclusivity check must not fire for a column's own type, or these become errors.
    indexKinds.foreach { kind =>
      val index = Index(s"exclusivity_idempotent_${kind.label}", exclusivitySchema, "parquet")
      kind.register(index)
      noException should be thrownBy kind.register(index)
    }
  }
}
