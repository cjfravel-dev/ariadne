package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

/** Trait providing file operations for Index instances.
  */
trait IndexFileOperations extends IndexMetadataOperations {
  self: Index =>

  /** Returns the stored schema of the index. */
  def storedSchema: StructType =
    DataType.fromJson(metadata.schema).asInstanceOf[StructType]

  /** Reads a set of files into a DataFrame based on the specified format.
    *
    * @param files
    *   A set of file paths to read.
    * @return
    *   A DataFrame containing the contents of the specified files.
    */
  protected def readFiles(files: Set[String]): DataFrame = {
    val baseDf = createBaseDataFrame(files)
    val withComputedIndexes = applyComputedIndexes(baseDf)
    applyExplodedFields(withComputedIndexes)
  }

  /** Creates a base DataFrame from the provided files using the stored format
    * and schema.
    *
    * @param files
    *   Set of file paths to read
    * @return
    *   DataFrame with data from the specified files
    */
  protected def createBaseDataFrame(files: Set[String]): DataFrame = {
    // Apply read options
    val configuredReader =
      metadata.read_options.asScala.foldLeft(spark.read.schema(storedSchema)) {
        case (r, (key, value)) => r.option(key, value)
      }

    // Load data based on format
    format match {
      case "csv"     => configuredReader.csv(files.toList: _*)
      case "parquet" => configuredReader.parquet(files.toList: _*)
      case "json"    => configuredReader.json(files.toList: _*)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported format: $format")
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
    metadata.computed_indexes.asScala.foldLeft(df) {
      case (tempDf, (colName, exprStr)) =>
        tempDf.withColumn(colName, expr(exprStr))
    }
  }

  /** Applies exploded field transformations to a DataFrame.
    *
    * @param df
    *   The DataFrame to transform
    * @return
    *   DataFrame with exploded field columns added
    */
  protected def applyExplodedFields(df: DataFrame): DataFrame = {
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