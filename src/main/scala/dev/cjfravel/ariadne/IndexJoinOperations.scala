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
    mutable.Map[(Set[String], Map[String, Seq[Any]]), DataFrame]()

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

  /** Creates filter conditions for DataFrame joins based on index values.
    *
    * @param joinColumnsToUse The columns to create filters for
    * @param columnMappings Mapping from join columns to storage columns
    * @param indexes The index values to filter on
    * @return Sequence of filter conditions
    */
  protected def createJoinFilters(
    joinColumnsToUse: Seq[String],
    columnMappings: Map[String, String],
    indexes: Map[String, Array[Any]]
  ): Seq[Column] = {
    joinColumnsToUse.collect {
      case joinColumn if indexes.get(columnMappings(joinColumn)).exists(_.nonEmpty) =>
        val storageColumn = columnMappings(joinColumn)
        val values = indexes(storageColumn)
        col(joinColumn).isin(values: _*)
    }
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

    // Get values from the user DataFrame using join column names
    val joinColumnsToUse = usingColumns.filter(col =>
      columnMappings.contains(col) && storageColumnsToUse.contains(columnMappings(col))
    )
    val filtered = df.select(joinColumnsToUse.map(col): _*)

    val indexes = joinColumnsToUse.map { joinColumn =>
      val distinctValues = filtered.select(joinColumn).distinct.collect.map(_.get(0))
      columnMappings(joinColumn) -> distinctValues
    }.toMap

    val files = locateFiles(indexes)
    logger.trace(s"Found ${files.size} files in index")
    val readIndex = readFiles(files)

    // Create filters using the extracted helper method
    val filters = createJoinFilters(joinColumnsToUse, columnMappings, indexes)

    val filteredReadIndex = if (filters.nonEmpty) {
      readIndex.filter(filters.reduce(_ && _))
    } else {
      readIndex
    }

    val cacheKey = (files, indexes.mapValues(_.toSeq))
    joinCache.getOrElseUpdate(cacheKey, filteredReadIndex.cache)
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