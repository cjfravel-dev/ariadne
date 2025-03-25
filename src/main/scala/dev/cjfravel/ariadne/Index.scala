package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.nio.charset.StandardCharsets
import scala.collection.mutable

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

  def format: String = {
    val currentMetadata = metadata
    currentMetadata.hcursor.downField("format").as[String] match {
      case Right(storedFormat) => storedFormat
      case Left(_)             => throw new MissingFormatException()
    }
  }

  private[ariadne] def format_=(format: String): Unit = {
    val currentMetadata = metadata
    val updatedMetadata =
      currentMetadata.deepMerge(Json.obj("format" -> Json.fromString(format)))
    writeMetadata(updatedMetadata)
  }

  private[ariadne] def metadataExists: Boolean = fs.exists(metadataFilePath)

  private var _metadata: Json = _
  private def metadata: Json = {
    if (_metadata == null) {
      _metadata = if (metadataExists) {
        try {
          val inputStream = fs.open(metadataFilePath)
          val jsonString =
            Source.fromInputStream(inputStream)(StandardCharsets.UTF_8).mkString
          parse(jsonString) match {
            case Right(json) => json
            case Left(_)     => throw new MetadataMissingOrCorruptException()
          }
        } catch {
          case _: Exception => throw new MetadataMissingOrCorruptException()
        }
      } else {
        throw new MetadataMissingOrCorruptException()
      }
    }
    _metadata
  }

  private def writeMetadata(json: Json): Unit = {
    val directoryPath = metadataFilePath.getParent
    if (!fs.exists(directoryPath)) fs.mkdirs(directoryPath)

    val jsonString = json.spaces2
    val outputStream = fs.create(metadataFilePath)
    outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
    outputStream.close()
    _metadata = null
  }

  def storedSchema: StructType = {
    val currentMetadata = metadata
    currentMetadata.hcursor.downField("schema").as[String] match {
      case Right(storedSchemaJson) =>
        try {
          DataType.fromJson(storedSchemaJson).asInstanceOf[StructType]
        } catch {
          case _: Exception => throw new SchemaParseError()
        }
      case Left(_) => throw new MissingSchemaException()
    }
  }

  def isFileAdded(fileName: String): Boolean = {
    metadata.hcursor.downField("files").focus match {
      case Some(files) =>
        files.asArray.exists(
          _.exists(file =>
            file.hcursor.get[String]("file").getOrElse("") == fileName
          )
        )
      case None => false
    }
  }

  def addFile(fileNames: String*): Unit = {
    val toAdd = fileNames.toList.diff(files.toList)
    if (toAdd.isEmpty) return

    val currentMetadata = metadata
    val timestamp = System.currentTimeMillis()
    val existingFiles = currentMetadata.hcursor
      .downField("files")
      .as[Vector[Json]]
      .getOrElse(Vector())

    val newFilesJson = toAdd.map { fileName =>
      Json.obj(
        "file" -> Json.fromString(fileName),
        "timestamp" -> Json.fromLong(timestamp),
        "indexed" -> Json.fromBoolean(false)
      )
    }

    val updatedMetadata = currentMetadata.deepMerge(
      Json.obj("files" -> Json.fromValues(existingFiles ++ newFilesJson))
    )
    writeMetadata(updatedMetadata)
  }

  def files: Set[String] = {
    metadata.hcursor.downField("files").focus match {
      case Some(files) =>
        files.asArray
          .getOrElse(Vector())
          .flatMap { file =>
            file.hcursor.get[String]("file").toOption
          }
          .toSet
      case None => Set()
    }
  }

  private[ariadne] def unindexedFiles: Set[String] = {
    metadata.hcursor.downField("files").focus match {
      case Some(files) =>
        files.asArray
          .getOrElse(Vector())
          .flatMap { file =>
            file.hcursor.get[Boolean]("indexed") match {
              case Right(false) => file.hcursor.get[String]("file").toOption
              case _            => None
            }
          }
          .toSet
      case None => Set()
    }
  }

  def addIndex(index: String): Unit = {
    if (indexes.contains(index)) return

    val updatedMetadata = metadata.hcursor.downField("indexes").focus match {
      case Some(indexesJson) =>
        val updatedIndexes =
          indexesJson.asArray.getOrElse(Vector()) :+ Json.fromString(index)
        metadata.deepMerge(
          Json.obj("indexes" -> Json.fromValues(updatedIndexes))
        )
      case None =>
        metadata.deepMerge(
          Json.obj("indexes" -> Json.arr(Json.fromString(index)))
        )
    }

    val finalMetadata = updatedMetadata.hcursor.downField("files").focus match {
      case Some(filesJson) =>
        val updatedFiles = filesJson.asArray.getOrElse(Vector()).map { file =>
          file.hcursor
            .downField("indexed")
            .withFocus(_ => Json.fromBoolean(false))
            .top
            .getOrElse(file)
        }
        updatedMetadata.deepMerge(
          Json.obj("files" -> Json.fromValues(updatedFiles))
        )
      case None => updatedMetadata
    }

    writeMetadata(finalMetadata)
  }

  private def setIndexedTrue: Unit = {
    val updatedMetadata = metadata.hcursor.downField("files").focus match {
      case Some(filesJson) =>
        val updatedFiles = filesJson.asArray.getOrElse(Vector()).map { file =>
          file.hcursor
            .downField("indexed")
            .withFocus(_ => Json.fromBoolean(true))
            .top
            .getOrElse(file)
        }
        metadata.deepMerge(Json.obj("files" -> Json.fromValues(updatedFiles)))
      case None => metadata
    }

    writeMetadata(updatedMetadata)
  }

  def indexes: Set[String] = {
    metadata.hcursor.downField("indexes").focus match {
      case Some(indexesJson) =>
        indexesJson.asArray.getOrElse(Vector()).flatMap(_.asString).toSet
      case None => Set()
    }
  }

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
      .distinct()

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
          .distinct()

        accumDF.union(filteredDF)
    }

    resultDF.distinct().collect().map(_.getString(0)).toSet
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

    schema match {
      case Some(s) =>
        if (metadataExists) {
          val currentMetadata = index.metadata
          currentMetadata.hcursor.downField("schema").as[String] match {
            case Right(storedSchema) =>
              if (storedSchema != s.json) {
                throw new IllegalArgumentException(
                  s"Stored schema does not match the provided schema for index: $name."
                )
              }
            case Left(_) =>
              index.writeMetadata(Json.obj("schema" -> Json.fromString(s.json)))
          }
        } else {
          index.writeMetadata(Json.obj("schema" -> Json.fromString(s.json)))
        }
      case None =>
        if (!metadataExists) {
          throw new SchemaNotProvidedException()
        }
    }
    format.foreach(index.format = _)
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
          filtered.select(column).distinct().collect().map(_.get(0))
        column -> distinctValues
      }.toMap
      val spark = df.sparkSession
      val files = index.locateFiles(indexes)
      val matchedFiles = cache.getOrElseUpdate(files, index.readFiles(files))
      df.join(matchedFiles, usingColumns, joinType)
    }
  }
}
