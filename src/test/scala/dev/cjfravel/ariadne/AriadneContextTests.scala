package dev.cjfravel.ariadne

class AriadneContextTests extends SparkTests {
  test("storagePath") {
    val contextUser = new AriadneContextUser {
      implicit def spark: org.apache.spark.sql.SparkSession = AriadneContextTests.this.spark
    }
    assert(contextUser.storagePath.toString === tempDir.toString)
  }
}
