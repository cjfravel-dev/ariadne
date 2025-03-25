package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import com.google.gson._
import com.google.gson.reflect.TypeToken
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import collection.JavaConverters._
import java.util
import java.util.Collections

case class Index private (
    spark: SparkSession,
    name: String,
    schema: Option[StructType]
) {
  private var _storagePath: Path = _
  def storagePath: Path = _storagePath

  private[ariadne] def storagePath_=(storagePath: Path): Unit = {
    _storagePath = storagePath
  }

  private var _fs: FileSystem = _
  def fs: FileSystem = {
    if (_fs == null) {
      _fs = FileSystem.get(
        storagePath.getParent.toUri,
        spark.sparkContext.hadoopConfiguration
      )
    }
    _fs
  }

  private def metadataFilePath: Path = new Path(storagePath, "metadata.json")
  private def indexFilePath: Path = new Path(storagePath, "index")

  def format: String = metadata.format

  private[ariadne] def format_=(newFormat: String): Unit = {
    val currentMetadata = metadata
    currentMetadata.format = newFormat
    writeMetadata(currentMetadata)
  }

  private[ariadne] def metadataExists: Boolean =
    fs.exists(metadataFilePath)

  private var _metadata: Metadata = _
  private def metadata: Metadata = {
    if (_metadata == null) {
      _metadata = if (metadataExists) {
        try {
          val inputStream = fs.open(metadataFilePath)
          val jsonString =
            Source.fromInputStream(inputStream)(StandardCharsets.UTF_8).mkString
          new Gson().fromJson(jsonString, classOf[Metadata])
        } catch {
          case _: Exception => throw new MetadataMissingOrCorruptException()
        }
      } else {
        throw new MetadataMissingOrCorruptException()
      }
    }
    _metadata
  }

  def files: Set[String] = metadata.files.asScala.map(_.file).toSet

  private def writeMetadata(metadata: Metadata): Unit = {
    val directoryPath = metadataFilePath.getParent
    if (!fs.exists(directoryPath)) fs.mkdirs(directoryPath)

    val jsonString = new Gson().toJson(metadata)
    val outputStream = fs.create(metadataFilePath)
    outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
    outputStream.flush()
    outputStream.close()
    _metadata = null
  }

  def storedSchema: StructType =
    DataType.fromJson(metadata.schema).asInstanceOf[StructType]

  def isFileAdded(fileName: String): Boolean = {
    metadata.files.asScala.exists(_.file == fileName)
  }

  def addFile(fileNames: String*): Unit = {
    val toAdd = fileNames.toList.diff(files.toList)
    if (toAdd.isEmpty) return

    val timestamp = System.currentTimeMillis()
    val newFiles = toAdd.map(FileMetadata(_, timestamp, indexed = false))

    val currentMetadata = metadata
    currentMetadata.files.addAll(newFiles.asJava)
    writeMetadata(currentMetadata)
  }

  private[ariadne] def unindexedFiles: Set[String] =
    metadata.files.asScala.filter(!_.indexed).map(_.file).toSet

  def addIndex(index: String): Unit = {
    if (metadata.indexes.contains(index)) return
    metadata.indexes.add(index)
    metadata.files.asScala.foreach(_.indexed = false)
    writeMetadata(metadata)
  }

  private def setIndexedTrue(): Unit = {
    metadata.files.asScala.foreach(_.indexed = true)
    writeMetadata(metadata)
  }

  def indexes: Set[String] = metadata.indexes.asScala.toSet

  private def index: DataFrame = {
    val deltaTable = DeltaTable.forPath(spark, indexFilePath.toString)
    deltaTable.toDF
  }

  private[ariadne] def printIndex(): Unit = {
    val df = index
    df.show(false)
    df.printSchema()
  }

  private[ariadne] def printMetadata: Unit = println(metadata)

  private def readFiles(files: Set[String]): DataFrame = {
    format match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .schema(storedSchema)
          .csv(files.toList: _*)
      case "parquet" =>
        spark.read.schema(storedSchema).parquet(files.toList: _*)
    }
  }

  def update: Unit = {
    val columns = indexes + "FileName"
    val df = readFiles(unindexedFiles)
      .withColumn("FileName", input_file_name)
      .select(columns.toList.map(col): _*)
      .distinct

    val aggExprs =
      indexes.toList.map(colName => collect_set(col(colName)).alias(colName))
    val groupedDf = df.groupBy("FileName").agg(aggExprs.head, aggExprs.tail: _*)

    if (!fs.exists(indexFilePath)) {
      groupedDf.write
        .format("delta")
        .mode("overwrite")
        .save(indexFilePath.toString)
    } else {
      val deltaTable = DeltaTable.forPath(spark, indexFilePath.toString)
      deltaTable
        .as("target")
        .merge(groupedDf.as("source"), "target.FileName = source.FileName")
        .whenMatched()
        .updateExpr(indexes.map(colName => colName -> s"source.$colName").toMap)
        .whenNotMatched()
        .insertAll()
        .execute()
    }

    setIndexedTrue
  }

  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
    val df = index
    val schema = StructType(
      Array(StructField("FileName", StringType, nullable = false))
    )
    val emptyDF =
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val resultDF = indexes.foldLeft(emptyDF) {
      case (accumDF, (column, values)) =>
        val filteredDF = df
          .select("FileName", column)
          .withColumn("Value", explode(col(column)))
          .where(col("Value").isin(values: _*))
          .select("FileName")
          .distinct

        accumDF.union(filteredDF)
    }

    resultDF.distinct.collect.map(_.getString(0)).toSet
  }
}

object Index {
  def storagePath(spark: SparkSession): String =
    spark.conf.get("spark.ariadne.storagePath")

  def checkStoragePath(spark: SparkSession): Boolean = {
    val path = new Path(storagePath(spark))
    val fs = FileSystem.get(
      path.getParent.toUri,
      spark.sparkContext.hadoopConfiguration
    )
    fs.exists(path)
  }

  def apply(
      spark: SparkSession,
      name: String,
      schema: StructType,
      format: String
  ): Index = apply(spark, name, Some(schema), Some(format))

  def apply(
      spark: SparkSession,
      name: String,
      schema: Option[StructType] = None,
      format: Option[String] = None
  ): Index = {
    val index = Index(spark, name, schema)
    index.storagePath = new Path(storagePath(spark), name)

    val metadataExists = index.metadataExists
    val metadata = if (metadataExists) {
      index.metadata
    } else {
      Metadata(
        null,
        null,
        Collections.emptyList[FileMetadata](),
        Collections.emptyList[String]()
      )
    }

    schema match {
      case Some(s) =>
        if (metadataExists) {
          if (metadata.schema != s.json) {
            throw new IllegalArgumentException(
              s"Stored schema does not match the provided schema for index: $name."
            )
          }
        } else {
          metadata.schema = s.json
        }
      case None =>
        if (!metadataExists) {
          throw new SchemaNotProvidedException()
        }
    }
    format match {
      case Some(f) =>
        if (metadataExists) {
          if (metadata.format != f) {
            throw new IllegalArgumentException(
              s"Stored format does not match the provided format for index: $name."
            )
          }
        } else {
          metadata.format = f
        }
      case None =>
        if (!metadataExists) {
          throw new MissingFormatException()
        }
    }
    index.writeMetadata(metadata)
    index
  }

  implicit class DataFrameOps(df: DataFrame) {
    private val cache = mutable.Map[Set[String], DataFrame]()
    def join(
        index: Index,
        usingColumns: Seq[String],
        joinType: String = "inner"
    ): DataFrame = {
      val indexesToUse = index.indexes.intersect(usingColumns.toSet).toSeq
      val filtered = df.select(indexesToUse.map(col): _*)
      val indexes = indexesToUse.map { column =>
        val distinctValues =
          filtered.select(column).distinct.collect.map(_.get(0))
        column -> distinctValues
      }.toMap
      val files = index.locateFiles(indexes)
      val matchedFiles = cache.getOrElseUpdate(files, index.readFiles(files))
      df.join(matchedFiles, usingColumns, joinType)
    }
  }
}
