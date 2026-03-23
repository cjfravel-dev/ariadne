package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.SchemaParseException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

/** Trait providing file I/O operations for [[Index]] instances.
  *
  * Handles reading data files in supported formats (CSV, Parquet, JSON),
  * applying computed indexes and exploded field transformations, and
  * managing column selection for optimized reads.
  *
  * '''Read pipeline:''' [[readFiles]] orchestrates the full read path:
  *  1. [[createBaseDataFrame]] — loads files using the stored schema, format, and read options
  *  2. [[applyComputedIndexes]] — adds derived columns via Spark SQL expressions
  *  3. [[applyExplodedFields]] — explodes nested array fields into top-level columns
  *  4. [[applyColumnSelection]] — prunes to user-selected columns if specified
  *
  * '''Format support:''' CSV, Parquet, and JSON. The format is stored in
  * [[IndexMetadata]] and determines which Spark reader method is used.
  * Read options (e.g., `header`, `delimiter`) are applied from metadata.
  *
  * '''Filename tracking:''' [[addFilenameColumn]] uses `input_file_name()` to
  * attach the source file path to each row. For single-file reads, a fallback
  * literal is used because Spark may report an empty filename.
  *
  * @see [[IndexMetadataOperations]] for metadata access
  * @see [[IndexBuildOperations]] for the build pipeline that calls these operations
  */
trait IndexFileOperations extends IndexMetadataOperations {
  self: Index =>

  /** Returns the stored schema of the index.
    *
    * Parses the JSON schema string from metadata into a Spark StructType.
    *
    * @return The StructType schema parsed from metadata
    * @throws dev.cjfravel.ariadne.exceptions.MissingSchemaException if the schema is null in metadata
    * @throws SchemaParseException if the schema string cannot be parsed as a StructType
    */
  def storedSchema: StructType = {
    if (metadata.schema == null) {
      throw new dev.cjfravel.ariadne.exceptions.MissingSchemaException()
    }
    DataType.fromJson(metadata.schema) match {
      case st: StructType => st
      case _ => throw new SchemaParseException()
    }
  }

  /** Reads a set of files into a DataFrame based on the specified format.
    *
    * Orchestrates the full read pipeline:
    *  1. [[createBaseDataFrame]] — loads files using stored schema, format, and read options
    *  2. [[applyComputedIndexes]] — adds derived columns via Spark SQL expressions
    *  3. [[applyExplodedFields]] — explodes nested array fields into top-level columns
    *  4. [[applyColumnSelection]] — prunes to user-selected columns if specified
    *
    * @param files
    *   A set of file paths to read
    * @return
    *   A DataFrame containing the contents of the specified files, with computed
    *   indexes, exploded fields, and column selection applied
    * @throws IllegalArgumentException
    *   if the stored format is not csv, parquet, or json (propagated from [[createBaseDataFrame]])
    */
  protected def readFiles(files: Set[String]): DataFrame = {
    logger.warn(s"Reading ${files.size} files in format '${metadata.format}'")
    if (debugEnabled) {
      logger.warn(s"[debug] readFiles: reading ${files.size} files, format: $format")
    }
    val readStart = System.currentTimeMillis()
    val baseDf = createBaseDataFrame(files)
    if (debugEnabled) {
      logger.warn(s"[debug] readFiles: createBaseDataFrame setup in ${System.currentTimeMillis() - readStart}ms")
    }
    val withComputedIndexes = applyComputedIndexes(baseDf)
    val withExplodedFields = applyExplodedFields(withComputedIndexes)
    
    // Apply column selection if specified
    val result = applyColumnSelection(withExplodedFields)
    if (debugEnabled) {
      logger.warn(s"[debug] readFiles: complete setup in ${System.currentTimeMillis() - readStart}ms, columns: ${result.schema.fieldNames.length}")
    }
    logger.warn(s"readFiles complete for ${files.size} file(s) in ${System.currentTimeMillis() - readStart}ms")
    result
  }

  /** Applies column selection if columns have been specified via select().
    * Only reads the explicitly selected columns.
    *
    * @param df The DataFrame to apply column selection to
    * @return DataFrame with selected columns or original DataFrame if no selection
    */
  protected def applyColumnSelection(df: DataFrame): DataFrame = {
    getSelectedColumns match {
      case Some(selectedCols) =>
        logger.debug(s"Selecting columns: ${selectedCols.mkString(", ")}")
        df.select(selectedCols.map(col): _*)
      case None =>
        df // No column selection specified, return full DataFrame
    }
  }

  /** Creates a base DataFrame from the provided files using the stored format
    * and schema. Applies any read options configured in the index metadata.
    *
    * Returns an empty DataFrame (with the stored schema) if all file paths are
    * blank after normalization.
    *
    * @param files
    *   Set of file paths to read
    * @return
    *   DataFrame with data from the specified files
    * @throws IllegalArgumentException
    *   if the stored format is not csv, parquet, or json
    */
  protected def createBaseDataFrame(files: Set[String]): DataFrame = {
    val normalizedFiles = files.map(_.trim).filter(_.nonEmpty)
    if (normalizedFiles.isEmpty) {
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        storedSchema
      )
    } else {
      // Apply read options
      val configuredReader =
        metadata.read_options.asScala.foldLeft(spark.read.schema(storedSchema)) {
          case (r, (key, value)) => r.option(key, value)
        }

      // Load data based on format
      format match {
        case "csv"     => configuredReader.csv(normalizedFiles.toList: _*)
        case "parquet" => configuredReader.parquet(normalizedFiles.toList: _*)
        case "json"    => configuredReader.json(normalizedFiles.toList: _*)
        case _ =>
          logger.warn(s"Unsupported format '${format}' for index — must be csv, parquet, or json")
          throw new IllegalArgumentException(s"Unsupported format: $format")
      }
    }
  }

  /** Adds a stable filename column to the DataFrame.
    *
    * Uses `input_file_name()` to capture the source file path. For single-file
    * reads where Spark may report an empty filename, falls back to the known file path.
    *
    * @param df The DataFrame to add the filename column to
    * @param files The set of file paths being read
    * @return DataFrame with a `filename` column added
    */
  protected def addFilenameColumn(df: DataFrame, files: Set[String]): DataFrame = {
    if (files.size == 1) {
      val fallbackFile = files.head
      val inputFile = input_file_name()
      df.withColumn(
        "filename",
        when(inputFile.isNull || length(trim(inputFile)) === 0, lit(fallbackFile))
          .otherwise(inputFile)
      )
    } else {
      df.withColumn("filename", input_file_name())
    }
  }

  /** Applies computed indexes to a DataFrame by adding computed columns.
    *
    * @param df
    *   The base DataFrame
    * @return
    *   DataFrame with computed index columns added
    */
  protected def applyComputedIndexes(df: DataFrame): DataFrame = {
    logger.debug(s"Applying ${metadata.computed_indexes.size()} computed index(es)")
    metadata.computed_indexes.asScala.foldLeft(df) {
      case (tempDf, (colName, exprStr)) =>
        tempDf.withColumn(colName, expr(exprStr))
    }
  }

  /** Applies exploded field transformations to a DataFrame.
    *
    * For each configured exploded field, extracts the nested field path from the
    * array column and explodes it into a top-level column. Note that `explode()`
    * (not `explode_outer()`) is used intentionally — rows with null or empty arrays
    * are dropped, which is correct for index building since there are no values to index.
    *
    * @param df
    *   The DataFrame to transform
    * @return
    *   DataFrame with exploded field columns added
    */
  protected def applyExplodedFields(df: DataFrame): DataFrame = {
    logger.debug(s"Applying ${metadata.exploded_field_indexes.size()} exploded field transform(s)")
    metadata.exploded_field_indexes.asScala.foldLeft(df) {
      case (tempDf, explodedField) =>
        tempDf.withColumn(
          explodedField.as_column,
          explode(
            col(s"${explodedField.array_column}.${explodedField.field_path}")
          )
        )
    }
  }
}
