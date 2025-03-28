package dev.cjfravel

import dev.cjfravel.ariadne.{AriadneContext, SparkTests}

class AriadneContextTests extends SparkTests {
  test("setSparkContext") {
    AriadneContext.setSparkSession(spark)
  }
}
