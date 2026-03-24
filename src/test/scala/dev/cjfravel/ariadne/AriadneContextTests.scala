package dev.cjfravel.ariadne

/** Tests for [[AriadneContextUser]] verifying storage path configuration
  * is correctly read from the SparkSession.
  */
class AriadneContextTests extends SparkTests {
  test("storagePath") {
    val contextUser = new AriadneContextUser {
      implicit def spark: org.apache.spark.sql.SparkSession = AriadneContextTests.this.spark
    }
    assert(contextUser.storagePath.toString === tempDir.toString)
  }
}
