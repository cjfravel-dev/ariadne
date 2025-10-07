package dev.cjfravel

import dev.cjfravel.ariadne.{AriadneContextUser, SparkTests}

class AriadneContextTests extends SparkTests {
  test("implicit spark is available") {
    // Test that implicit spark is properly set up
    val contextUser = new AriadneContextUser {
      implicit def spark: org.apache.spark.sql.SparkSession = AriadneContextTests.this.spark
    }
    assert(contextUser.storagePath != null)
    assert(contextUser.fs != null)
  }
}
