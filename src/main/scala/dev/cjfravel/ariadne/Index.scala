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
import org.apache.logging.log4j.{Logger, LogManager}
import dev.cjfravel.ariadne.Index.DataFrameOps
import org.apache.spark.SparkConf

/** Represents an Index for managing metadata and file-based indexes in Apache
  * Spark.
  *
  * This class provides methods to add, locate, and manage file-based indexing
  * in Spark using Delta Lake. It supports schema enforcement, metadata
  * persistence, and file tracking.
  *
  * @constructor
  *   Private: use the factory methods in the companion object.
  * @param spark
  *   The SparkSession instance.
  * @param name
  *   The name of the index.
  * @param schema
  *   The optional schema of the index.
  */
case class Index private (
    name: String,
    schema: Option[StructType]
) extends AriadneContextUser {
  val logger = LogManager.getLogger("ariadne")

  private def fileList: FileList = FileList(Index.fileListName(name))

  /** Path to the storage location of the index. */
  override def storagePath: Path = new Path(Index.storagePath, name)

  /** Hadoop path for the index delta table */
  private def indexFilePath: Path = new Path(storagePath, "index")

  /** Hadoop path for the index delta table */
  private def metadataFilePath: Path = new Path(storagePath, "metadata.json")

  /** Returns the format of the stored data. */
  def format: String = metadata.format

  /** Updates the format of the stored data.
    * @param newFormat
    *   The new format to set.
    */
  private def format_=(newFormat: String): Unit = {
    val currentMetadata = metadata
    currentMetadata.format = newFormat
    writeMetadata(currentMetadata)
  }

  /** Checks if metadata exists in the storage location.
    * @return
    *   True if metadata exists, otherwise false.
    */
  private def metadataExists: Boolean =
    exists(metadataFilePath)

  private var _metadata: Metadata = _

  /** Retrieves the stored metadata for the index.
    *
    * @return
    *   Metadata associated with the index.
    * @throws MetadataMissingOrCorruptException
    *   if metadata is missing or cannot be parsed.
    */
  private def metadata: Metadata = {
    if (_metadata == null) {
      _metadata = if (metadataExists) {
        try {
          val inputStream = open(metadataFilePath)
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
    logger.trace(s"Read metadata from ${metadataFilePath.toString}")
    _metadata
  }

  /** Writes metadata to the storage location.
    * @param metadata
    *   The metadata to write.
    */
  private def writeMetadata(metadata: Metadata): Unit = {
    val directoryPath = metadataFilePath.getParent
    if (!exists(directoryPath)) fs.mkdirs(directoryPath)

    val jsonString = new Gson().toJson(metadata)
    val outputStream = fs.create(metadataFilePath)
    outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
    outputStream.flush()
    outputStream.close()
    _metadata = null
    logger.trace(s"Wrote metadata to ${metadataFilePath.toString}")
  }

  /** Returns the stored schema of the index. */
  def storedSchema: StructType =
    DataType.fromJson(metadata.schema).asInstanceOf[StructType]

  def hasFile(fileName: String): Boolean = fileList.hasFile(fileName)

  def addFile(fileNames: String*): Unit = fileList.addFile(fileNames: _*)

  /** Helper function to get a list of files that haven't yet been indexed
    *
    * @return
    *   Set of filenames
    */
  private[ariadne] def unindexedFiles: Set[String] = unindexedFiles(spark)
  private[ariadne] def unindexedFiles(spark: SparkSession): Set[String] = {
    val files = fileList.files
    if (files.isEmpty) {
      return Set()
    }
    import spark.implicits._
    index match {
      case Some(df) =>
        files
          .join(df, Seq("filename"), "left_anti")
          .as[FileEntry]
          .map(_.filename)
          .collect()
          .toSet
      case None => files.map(_.filename).collect().toSet
    }
  }

  /** Adds an index entry.
    * @param index
    *   The index entry to add.
    */
  def addIndex(index: String): Unit = {
    if (metadata.indexes.contains(index)) return
    metadata.indexes.add(index)
    writeMetadata(metadata)
  }

  /** Helper function to get the indexes
    *
    * @return
    *   Set of column names to be indexed
    */
  def indexes: Set[String] = metadata.indexes.asScala.toSet

  /** Helper function to load the index
    *
    * @return
    *   DataFrame containing latest version of the index
    */
  private def index: Option[DataFrame] = {
    delta(indexFilePath) match {
      case Some(delta) => Some(delta.toDF)
      case None        => None
    }
  }

  /** Prints the index DataFrame to the console, including its schema.
    *
    * This method retrieves the latest version of the index stored in Delta
    * Lake, displays its contents, and prints its schema.
    */
  private[ariadne] def printIndex(): Unit = {
    index match {
      case Some(df) =>
        df.show(false)
        df.printSchema()
      case None =>
    }
  }

  /** Prints the metadata associated with the index to the console.
    *
    * This metadata contains details about the index, including its schema,
    * format, and tracked files.
    */
  private[ariadne] def printMetadata: Unit = println(metadata)

  /** Reads a set of files into a DataFrame based on the specified format.
    *
    * @param files
    *   A set of file paths to read.
    * @return
    *   A DataFrame containing the contents of the specified files.
    */
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

  /** Updates the index with new files. */
  def update: Unit = {
    val columns = indexes + "filename"
    val df = readFiles(unindexedFiles)
      .withColumn("filename", input_file_name)
      .select(columns.toList.map(col): _*)
      .distinct

    val aggExprs =
      indexes.toList.map(colName => collect_set(col(colName)).alias(colName))
    val groupedDf = df.groupBy("filename").agg(aggExprs.head, aggExprs.tail: _*)

    delta(indexFilePath) match {
      case Some(delta) =>
        delta
          .as("target")
          .merge(groupedDf.as("source"), "target.filename = source.filename")
          .whenMatched()
          .updateExpr(
            indexes.map(colName => colName -> s"source.$colName").toMap
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      case None =>
        groupedDf.write
          .format("delta")
          .mode("overwrite")
          .save(indexFilePath.toString)
    }
  }

  /** Locates files based on index values.
    * @param indexes
    *   A map of index column names to their values.
    * @return
    *   A set of file names matching the criteria.
    */
  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
    index match {
      case Some(df) =>
        val schema = StructType(
          Array(StructField("filename", StringType, nullable = false))
        )
        val emptyDF =
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[Row],
            schema
          )

        val resultDF = indexes.foldLeft(emptyDF) {
          case (accumDF, (column, values)) =>
            val filteredDF = df
              .select("filename", column)
              .withColumn("value", explode(col(column)))
              .where(col("value").isin(values: _*))
              .select("filename")
              .distinct

            accumDF.union(filteredDF)
        }

        resultDF.distinct.collect.map(_.getString(0)).toSet
      case None => Set()
    }
  }

  private val joinCache = mutable.Map[Set[String], DataFrame]()

  /** Retrieves and caches a DataFrame containing indexed data relevant to the
    * given DataFrame.
    *
    * This method determines which index columns exist in the provided
    * DataFrame, retrieves the relevant indexed files, and loads them into a
    * DataFrame.
    *
    * @param df
    *   The DataFrame to match against the index.
    * @param usingColumns
    *   The columns used for the join.
    * @return
    *   A DataFrame containing data from indexed files that match the provided
    *   DataFrame.
    */
  private def joinDf(df: DataFrame, usingColumns: Seq[String]): DataFrame = {
    val indexesToUse = this.indexes.intersect(usingColumns.toSet).toSeq
    logger.trace(s"Found indexes for ${indexesToUse.mkString(",")}")
    val filtered = df.select(indexesToUse.map(col): _*)
    val indexes = indexesToUse.map { column =>
      val distinctValues =
        filtered.select(column).distinct.collect.map(_.get(0))
      column -> distinctValues
    }.toMap
    val files = locateFiles(indexes)
    logger.trace(s"Found ${files.size} files in index")
    val readIndex = readFiles(files)
    val filters =
      indexes.collect {
        case (column, values) if values.nonEmpty =>
          col(column).isin(values: _*)
      }

    val filteredReadIndex =
      if (filters.nonEmpty) {
        readIndex.filter(filters.reduce(_ && _))
      } else {
        readIndex
      }

    joinCache.getOrElseUpdate(files, filteredReadIndex.cache)
  }

  /** Joins a DataFrame with the index.
    * @param df
    *   The DataFrame to join.
    * @param usingColumns
    *   The columns to use for the join.
    * @param joinType
    *   The type of join (default is "inner").
    * @return
    *   The resulting joined DataFrame.
    */
  def join(
      df: DataFrame,
      usingColumns: Seq[String],
      joinType: String = "inner"
  ): DataFrame = {
    val indexDf = joinDf(df, usingColumns)
    indexDf.join(df, usingColumns, joinType)
  }
}

/** Companion object for the Index class.
  */
object Index extends AriadneContextUser {
  override def storagePath: Path = new Path(super.storagePath, "indexes")

  def fileListName(name: String): String = s"[ariadne_index] $name"

  def exists(name: String): Boolean =
    FileList.exists(fileListName(name)) || super.exists(
      new Path(super.storagePath, name)
    )

  def remove(name: String): Boolean = {
    if (!exists(name)) {
      throw new IndexNotFoundException(name)
    }

    val fileListRemoved = FileList.remove(fileListName(name))
    delete(new Path(super.storagePath, name)) || fileListRemoved
  }

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String
  ): Index = apply(name, Some(schema), Some(format), false)

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @param allowSchemaMismatch
    *   The allows schema to be a mismatch.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      allowSchemaMismatch: Boolean
  ): Index = apply(name, Some(schema), Some(format), allowSchemaMismatch)

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The optional schema.
    * @param format
    *   The optional format.
    * @param allowSchemaMismatch
    *   The optional flag to allow new schema.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: Option[StructType] = None,
      format: Option[String] = None,
      allowSchemaMismatch: Boolean = false
  ): Index = {
    val index = Index(name, schema)

    val metadataExists = index.metadataExists
    val metadata = if (metadataExists) {
      index.metadata
    } else {
      Metadata(
        null,
        null,
        Collections.emptyList[String]()
      )
    }

    schema match {
      case Some(s) =>
        if (metadataExists) {
          if (allowSchemaMismatch) {
            if (metadata.schema != s.json) {
              metadata.indexes.forEach(col => {
                if (!SchemaHelper.fieldExists(s, col)) {
                  throw new IndexNotFoundInNewSchemaException(col)
                }
              })
            }
            metadata.schema = s.json
          } else if (metadata.schema != s.json) {
            throw new SchemaMismatchException()
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
            throw new FormatMismatchException()
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

  /** Implicit class for enhancing DataFrame operations with index based
    * operations.
    */
  implicit class DataFrameOps(df: DataFrame) {

    /** Joins the DataFrame with an Index.
      * @param index
      *   The Index instance.
      * @param usingColumns
      *   The columns to use for the join.
      * @param joinType
      *   The type of join (default is "inner").
      * @return
      *   The joined DataFrame.
      */
    def join(
        index: Index,
        usingColumns: Seq[String],
        joinType: String = "inner"
    ): DataFrame = {
      val indexDf = index.joinDf(df, usingColumns)
      df.join(indexDf, usingColumns, joinType)
    }
  }
}
