package dev.cjfravel.ariadne

class FileListTests extends SparkTests {
  test("addFile") {
    val filelist = FileList("test")
    val path = resourcePath("/data/table1_part0.csv")
    assert(filelist.hasFile(path) === false)
    filelist.addFile(path)
    assert(filelist.hasFile(path) === true)
  }

  test("addFile(s)") {
    val filelist = FileList("test2")
    val paths = Array(
      resourcePath("/data/table1_part0.csv"),
      resourcePath("/data/table1_part1.csv")
    )
    paths.foreach(path => assert(filelist.hasFile(path) === false))
    filelist.addFile(paths: _*)
    paths.foreach(path => assert(filelist.hasFile(path) === true))

    // manual logger test
    filelist.addFile(paths: _*)
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
