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
) {
  val logger = LogManager.getLogger("ariadne")

  /** Path to the storage location of the index. */
  private def storagePath: Path = new Path(Context.storagePath, name)

  /** Hadoop path for the metadata file */
  private def metadataFilePath: Path = new Path(storagePath, "metadata.json")

  /** Hadoop path for the index delta table */
  private def indexFilePath: Path = new Path(storagePath, "index")

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
    Context.fs.exists(metadataFilePath)

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
          val inputStream = Context.fs.open(metadataFilePath)
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

  /** Returns a set of file names stored in the index. */
  def files: Set[String] = metadata.files.asScala.map(_.file).toSet

  /** Writes metadata to the storage location.
    * @param metadata
    *   The metadata to write.
    */
  private def writeMetadata(metadata: Metadata): Unit = {
    val directoryPath = metadataFilePath.getParent
    if (!Context.fs.exists(directoryPath)) Context.fs.mkdirs(directoryPath)

    val jsonString = new Gson().toJson(metadata)
    val outputStream = Context.fs.create(metadataFilePath)
    outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8))
    outputStream.flush()
    outputStream.close()
    _metadata = null
    logger.trace(s"Wrote metadata to ${metadataFilePath.toString}")
  }

  /** Returns the stored schema of the index. */
  def storedSchema: StructType =
    DataType.fromJson(metadata.schema).asInstanceOf[StructType]

  /** Helper function to see if a file was already added
    *
    * @param fileName
    * @return
    *   True is the file is included already
    */
  def isFileAdded(fileName: String): Boolean = {
    metadata.files.asScala.exists(_.file == fileName)
  }

  /** Adds files to the index.
    * @param fileNames
    *   The names of the files to add.
    */
  def addFile(fileNames: String*): Unit = {
    val toAdd = fileNames.toList.diff(files.toList)
    if (toAdd.isEmpty) {
      logger.warn("All files were already added")
      return
    }

    val timestamp = System.currentTimeMillis()
    val newFiles = toAdd.map(FileMetadata(_, timestamp, indexed = false))

    val currentMetadata = metadata
    currentMetadata.files.addAll(newFiles.asJava)
    writeMetadata(currentMetadata)
    logger.trace(s"Added ${toAdd.length} files")
  }

  /** Helper function to get a list of files that haven't yet been indexed
    *
    * @return
    *   Set of filenames
    */
  private[ariadne] def unindexedFiles: Set[String] =
    metadata.files.asScala.filter(!_.indexed).map(_.file).toSet

  /** Adds an index entry.
    * @param index
    *   The index entry to add.
    */
  def addIndex(index: String): Unit = {
    if (metadata.indexes.contains(index)) return
    metadata.indexes.add(index)
    metadata.files.asScala.foreach(_.indexed = false)
    writeMetadata(metadata)
  }

  /** Sets all files as indexed */
  private def setIndexedTrue(): Unit = {
    metadata.files.asScala.foreach(_.indexed = true)
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
  private def index: DataFrame = {
    val deltaTable = DeltaTable.forPath(Context.spark, indexFilePath.toString)
    deltaTable.toDF
  }

  /** Prints the index DataFrame to the console, including its schema.
    *
    * This method retrieves the latest version of the index stored in Delta
    * Lake, displays its contents, and prints its schema.
    */
  private[ariadne] def printIndex(): Unit = {
    val df = index
    df.show(false)
    df.printSchema()
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
        Context.spark.read
          .option("header", "true")
          .schema(storedSchema)
          .csv(files.toList: _*)
      case "parquet" =>
        Context.spark.read.schema(storedSchema).parquet(files.toList: _*)
    }
  }

  /** Updates the index with new files. */
  def update: Unit = {
    val columns = indexes + "FileName"
    val df = readFiles(unindexedFiles)
      .withColumn("FileName", input_file_name)
      .select(columns.toList.map(col): _*)
      .distinct

    val aggExprs =
      indexes.toList.map(colName => collect_set(col(colName)).alias(colName))
    val groupedDf = df.groupBy("FileName").agg(aggExprs.head, aggExprs.tail: _*)

    if (!Context.exists(indexFilePath)) {
      groupedDf.write
        .format("delta")
        .mode("overwrite")
        .save(indexFilePath.toString)
    } else {
      val deltaTable = DeltaTable.forPath(Context.spark, indexFilePath.toString)
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

  /** Locates files based on index values.
    * @param indexes
    *   A map of index column names to their values.
    * @return
    *   A set of file names matching the criteria.
    */
  def locateFiles(indexes: Map[String, Array[Any]]): Set[String] = {
    val df = index
    val schema = StructType(
      Array(StructField("FileName", StringType, nullable = false))
    )
    val emptyDF =
      Context.spark.createDataFrame(
        Context.spark.sparkContext.emptyRDD[Row],
        schema
      )

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
    joinCache.getOrElseUpdate(files, readFiles(files))
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
object Index {

  /** Checks if an index with the given name exists.
    * @param spark
    *   The SparkSession instance.
    * @param name
    *   The name of the index.
    * @return
    *   True if the index exists, otherwise false.
    */
  def exists(spark: SparkSession, name: String): Boolean =
    Context.exists(new Path(Context.storagePath, name))

  /** Removes the index with the given name.
    * @param spark
    *   The SparkSession instance.
    * @param name
    *   The name of the index.
    * @return
    *   True if the index was successfully removed, otherwise false.
    */
  def remove(spark: SparkSession, name: String): Boolean = {
    if (!exists(spark, name)) {
      throw new IndexNotFoundException(name)
    }

    Context.delete(new Path(Context.storagePath, name))
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
        Collections.emptyList[FileMetadata](),
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
              metadata.files.asScala.foreach(_.indexed = false)
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
