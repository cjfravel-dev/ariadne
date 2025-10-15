package dev.cjfravel.ariadne

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
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
    logger.trace(s"Found indexes for ${storageColumnsToUse.mkString(",")}")

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

    val indexExploded = joinColumnsToUse.foldLeft(indexDf) { (accumDf, joinCol) =>
      val storageCol = columnMappings(joinCol)
      accumDf.withColumn(s"${storageCol}_exploded", explode(col(storageCol)))
    }

    val joinConditions = joinColumnsToUse.map { joinCol =>
      val storageCol = columnMappings(joinCol)
      col(s"${storageCol}_exploded") === distinctValuesDf(joinCol)
    }

    val matchingFilesDf = indexExploded
      .join(broadcast(distinctValuesDf), joinConditions.reduce(_ && _))
      .select("filename")
      .distinct

    val files = matchingFilesDf.collect().map(_.getString(0)).toSet
    val totalFiles = indexDf.select("filename").distinct.count()
    logger.info(s"Found ${files.size} matching files from $totalFiles total files")

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