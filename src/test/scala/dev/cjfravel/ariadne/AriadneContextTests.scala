package dev.cjfravel.ariadne

class AriadneContextTests extends SparkTests {
  test("storagePath") {
    assert(AriadneContext.storagePath.toString === tempDir.toString)
  }
}
