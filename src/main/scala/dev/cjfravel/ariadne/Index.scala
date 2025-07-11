package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{Logger, LogManager}
import dev.cjfravel.ariadne.Index.DataFrameOps
import com.google.gson.Gson
import scala.collection.JavaConverters._
import java.util
import java.util.Collections

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
) extends IndexQueryOperations {

  private def fileList: FileList = FileList(IndexPathUtils.fileListName(name))

  /** Path to the storage location of the index. */
  override def storagePath: Path = new Path(IndexPathUtils.storagePath, name)

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
          .select("filename")
          .as[String]
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

  /** Adds an exploded field index entry.
    * @param arrayColumn
    *   The array column to index.
    * @param fieldPath
    *   The field path to extract from array elements (e.g., "id" or
    *   "profile.user_id").
    * @param asColumn
    *   The column name to use in joins.
    */
  def addExplodedFieldIndex(
      arrayColumn: String,
      fieldPath: String,
      asColumn: String
  ): Unit = {
    // Check if this asColumn is already used
    if (indexes.contains(asColumn)) return

    val explodedFieldMapping =
      ExplodedFieldMapping(arrayColumn, fieldPath, asColumn)
    metadata.exploded_field_indexes.add(explodedFieldMapping)
    writeMetadata(metadata)
  }

  /** Helper function to get all index column names that can be used in joins.
    *
    * @return
    *   Set of all column names that can be used in joins
    */
  def indexes: Set[String] =
    metadata.indexes.asScala.toSet ++
      metadata.computed_indexes.keySet().asScala ++
      metadata.exploded_field_indexes.asScala.map(_.as_column).toSet

  def addComputedIndex(name: String, sql_expression: String): Unit = {
    if (metadata.computed_indexes.containsKey(name)) return
    metadata.computed_indexes.put(name, sql_expression)
    writeMetadata(metadata)
  }

  /** Updates the index with new files. */
  def update: Unit = {
    val unindexed = unindexedFiles
    if (unindexed.nonEmpty) {
      logger.trace(s"Updating index for ${unindexed.size} files")

      // Read base data
      val baseDf = createBaseDataFrame(unindexed)
      val withComputedIndexes = applyComputedIndexes(baseDf)
      val withFilename =
        withComputedIndexes.withColumn("filename", input_file_name)

      // Build indexes using the extracted methods
      val regularIndexesDf = buildRegularIndexes(withFilename)
      val finalDf = buildExplodedFieldIndexes(withFilename, regularIndexesDf)

      // Handle large indexes and merge to Delta
      handleLargeIndexes(finalDf)
      mergeToDelta(finalDf)
    }
  }
}

/** Companion object for the Index class.
  */
object Index {
  def fileListName(name: String): String = IndexPathUtils.fileListName(name)

  def exists(name: String): Boolean = IndexPathUtils.exists(name)

  def remove(name: String): Boolean = IndexPathUtils.remove(name)

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

  /** Factory method to create an Index instance with read options.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @param readOptions
    *   Map of read options for format-specific configuration.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      readOptions: Map[String, String]
  ): Index = apply(name, Some(schema), Some(format), false, Some(readOptions))

  /** Factory method to create an Index instance with read options.
    * @param name
    *   The name of the index.
    * @param schema
    *   The schema.
    * @param format
    *   The format.
    * @param allowSchemaMismatch
    *   The allows schema to be a mismatch.
    * @param readOptions
    *   Map of read options for format-specific configuration.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: StructType,
      format: String,
      allowSchemaMismatch: Boolean,
      readOptions: Map[String, String]
  ): Index = apply(
    name,
    Some(schema),
    Some(format),
    allowSchemaMismatch,
    Some(readOptions)
  )

  /** Factory method to create an Index instance.
    * @param name
    *   The name of the index.
    * @param schema
    *   The optional schema.
    * @param format
    *   The optional format.
    * @param allowSchemaMismatch
    *   The optional flag to allow new schema.
    * @param readOptions
    *   Optional map of read options for format-specific configuration.
    * @return
    *   An Index instance.
    */
  def apply(
      name: String,
      schema: Option[StructType] = None,
      format: Option[String] = None,
      allowSchemaMismatch: Boolean = false,
      readOptions: Option[Map[String, String]] = None
  ): Index = {
    val index = Index(name, schema)

    val metadataExists = index.metadataExists
    val metadata = if (metadataExists) {
      index.metadata
    } else {
      IndexMetadata(
        null,
        null,
        new util.ArrayList[String](),
        new util.HashMap[String, String](),
        new util.ArrayList[ExplodedFieldMapping](),
        new util.HashMap[String, String]()
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

    // Handle read options
    readOptions match {
      case Some(options) =>
        if (metadataExists) {
          // Merge with existing options, with new options taking precedence
          import collection.JavaConverters._
          options.foreach { case (key, value) =>
            metadata.read_options.put(key, value)
          }
        } else {
          // Set initial options
          import collection.JavaConverters._
          options.foreach { case (key, value) =>
            metadata.read_options.put(key, value)
          }
        }
      case None => // Keep existing options
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
