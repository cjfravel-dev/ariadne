package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import scala.collection.mutable
import scala.collection.JavaConverters._

/** Trait providing join operations for Index instances.
  */
trait IndexJoinOperations extends IndexBuildOperations {
  self: Index =>

  private val joinCache =
    mutable.Map[(Set[String], String), DataFrame]()

  /** Maps join column names to their corresponding storage column names.
    *
    * @param joinColumns The column names used in joins
    * @return Map from join column to storage column
    */
  protected def mapJoinColumnsToStorage(joinColumns: Seq[String]): Map[String, String] = {
    joinColumns.map { joinCol =>
      // Check if this is an exploded field column
      val explodedMapping = metadata.exploded_field_indexes.asScala.find(_.as_column == joinCol)
      explodedMapping match {
        case Some(mapping) => joinCol -> mapping.array_column
        case None => joinCol -> joinCol
      }
    }.toMap
  }

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
  protected def joinDf(df: DataFrame, usingColumns: Seq[String]): DataFrame = {
    // Map join columns to storage columns
    val columnMappings = mapJoinColumnsToStorage(usingColumns)

    val storageColumnsToUse = columnMappings.values.toSet.intersect(this.storageColumns)
    logger.warn(s"Found indexes for ${storageColumnsToUse.mkString(",")}")

    // Get columns to use for join
    val joinColumnsToUse = usingColumns.filter(col =>
      columnMappings.contains(col) && storageColumnsToUse.contains(columnMappings(col))
    )

    if (joinColumnsToUse.isEmpty) {
      logger.warn(s"No join columns found in index")
      return spark.emptyDataFrame
    }

    val distinctValuesDf = df.select(joinColumnsToUse.map(col): _*).distinct

    // Get the full index DataFrame
    val indexDf = index.getOrElse(
      throw new IllegalStateException(s"Index not found for $name")
    )


    val matchingFilesDf = if (joinColumnsToUse.length == 1) {
      // Single column case - simpler and more efficient
      val joinCol = joinColumnsToUse.head
      val storageCol = columnMappings(joinCol)

      // Check if column exists and is an array type
      val colSchema = indexDf.schema.fields.find(_.name == storageCol)
      colSchema match {
        case Some(field) if field.dataType.isInstanceOf[ArrayType] =>
          // Use exists to check if any value in distinctValuesDf exists in the index array
          indexDf.alias("idx")
            .join(
              distinctValuesDf.alias("vals"),
              array_contains(col(s"idx.$storageCol"), col(s"vals.$joinCol"))
            )
            .select("idx.filename")
            .distinct
        case Some(field) =>
          // Direct comparison for non-array columns
          indexDf.alias("idx")
            .join(
              distinctValuesDf.alias("vals"),
              col(s"idx.$storageCol") === col(s"vals.$joinCol")
            )
            .select("idx.filename")
            .distinct
        case None =>
          logger.error(s"Column $storageCol not found in index")
          spark.emptyDataFrame.select(lit("").as("filename")).limit(0)
      }
    } else {
      // Multi-column case - need to check all columns match
      val joinConditions = joinColumnsToUse.map { joinCol =>
        val storageCol = columnMappings(joinCol)
        val colSchema = indexDf.schema.fields.find(_.name == storageCol)

        colSchema match {
          case Some(field) if field.dataType.isInstanceOf[ArrayType] =>
            array_contains(col(s"idx.$storageCol"), col(s"vals.$joinCol"))
          case Some(field) =>
            col(s"idx.$storageCol") === col(s"vals.$joinCol")
          case None =>
            logger.error(s"Column $storageCol not found in index")
            lit(false)
        }
      }

      indexDf.alias("idx")
        .join(
          distinctValuesDf.alias("vals"),
          joinConditions.reduce(_ && _)
        )
        .select("idx.filename")
        .distinct
    }

    val files = matchingFilesDf.collect().map(_.getString(0)).toSet
    val totalFiles = indexDf.select("filename").distinct.count()
    logger.warn(s"Found ${files.size} matching files from $totalFiles total files")

    if (files.isEmpty) {
      logger.warn(s"No files found matching join criteria")
      // Return empty DataFrame with the correct schema from distinctValuesDf
      return distinctValuesDf.limit(0)
    }

    val readIndex = readFiles(files)

    val cacheKey = (files, joinColumnsToUse.mkString(","))
    joinCache.getOrElseUpdate(cacheKey, {
      readIndex.join(distinctValuesDf, joinColumnsToUse, "inner").cache
    })
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