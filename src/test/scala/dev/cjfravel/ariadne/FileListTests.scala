package dev.cjfravel.ariadne

import java.sql.Timestamp

/**
 * Tests for [[FileList]] covering file addition (single and batch), existence checks, and removal from the tracked file
 * list.
 */
class FileListTests extends SparkTests {
  test("addFile") {
    val filelist = FileList("test")
    val path = resourcePath("/data/table1_part0.csv")
    assert(filelist.hasFile(path) === false)
    filelist.addFile(path)
    assert(filelist.hasFile(path) === true)
  }

  test("addFile with no filenames remains a no-op") {
    val name = "empty_add"
    FileList(name).addFile()
    assert(FileList.exists(name) === false)
  }

  test("addFile(s)") {
    val filelist = FileList("test2")
    val paths = Array(resourcePath("/data/table1_part0.csv"), resourcePath("/data/table1_part1.csv"))
    paths.foreach(path => assert(filelist.hasFile(path) === false))
    filelist.addFile(paths: _*)
    paths.foreach(path => assert(filelist.hasFile(path) === true))

    // manual logger test
    filelist.addFile(paths: _*)
  }

  test("addFile preserves the original timestamp when a batch mixes existing and new files") {
    val filelist = FileList("mixed_add")
    val existing = resourcePath("/data/table1_part0.csv")
    val added = resourcePath("/data/table1_part1.csv")
    filelist.addFile(existing)
    val originalTimestamp =
      filelist.files.where(s"filename = '$existing'").select("addedAt").head().getAs[Timestamp](0)

    Thread.sleep(10)
    filelist.addFile(existing, added)

    val timestamps = filelist.files.collect().map(row => row.getString(0) -> row.getAs[Timestamp](1)).toMap
    assert(timestamps.keySet === Set(existing, added))
    assert(timestamps(existing) === originalTimestamp)
  }

  test("exists") {
    val filelist = FileList("exists")
    val path = resourcePath("/data/table1_part0.csv")
    filelist.addFile(path)
    assert(FileList.exists("exists") === true)

    assert(FileList.exists("doesntexist") === false)
  }

  test("remove") {
    val filelist = FileList("toremove")
    val path = resourcePath("/data/table1_part0.csv")
    filelist.addFile(path)
    assert(FileList.exists("toremove") === true)
    FileList.remove("toremove")
    assert(FileList.exists("toremove") === false)
  }
}
