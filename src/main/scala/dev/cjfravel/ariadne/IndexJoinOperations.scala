package dev.cjfravel.ariadne

import dev.cjfravel.ariadne.exceptions.ColumnNotFoundException
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
    val bloomColumnSet = bloomColumns
    
    joinColumns.map { joinCol =>
      // Check if this is a bloom filter column
      if (bloomColumnSet.contains(joinCol)) {
        joinCol -> (bloomColumnPrefix + joinCol)
      } else {
        // Check if this is an exploded field column
        val explodedMapping = metadata.exploded_field_indexes.asScala.find(_.as_column == joinCol)
        explodedMapping match {
          case Some(mapping) => joinCol -> mapping.array_column
          case None => joinCol -> joinCol
        }
      }
    }.toMap
  }

  /** Threshold for using broadcast join filtering instead of isin() predicates.
    * When the number of distinct values is at or below this threshold, we use a broadcast join approach.
    * When values exceed this threshold, we use regular join approach to avoid broadcasting large datasets.
    * Uses the broadcastJoinThreshold from AriadneContextUser.
    */
  protected def largeFilterThreshold: Long = broadcastJoinThreshold

  /** Creates filter conditions for DataFrame joins based on index values.
    * For large value sets, uses a join-based approach instead of isin() predicates.
    *
    * @param readIndex The DataFrame to filter
    * @param joinColumnsToUse The columns to create filters for
    * @param columnMappings Mapping from join columns to storage columns
    * @param indexes The index values to filter on
    * @return Filtered DataFrame
    */
  protected def applyJoinFilters(
    readIndex: DataFrame,
    joinColumnsToUse: Seq[String],
    columnMappings: Map[String, String],
    indexes: Map[String, Array[Any]]
  ): DataFrame = {
    val filtersToApply = joinColumnsToUse.filter { joinColumn =>
      indexes.get(columnMappings(joinColumn)).exists(_.nonEmpty)
    }
    
    if (filtersToApply.isEmpty) {
      return readIndex
    }
    
    // Check if any filter has a large number of values
    val hasLargeFilter = filtersToApply.exists { joinColumn =>
      val storageColumn = columnMappings(joinColumn)
      indexes(storageColumn).length > largeFilterThreshold
    }
    
    if (hasLargeFilter) {
      // Use traditional isin() filters for large value sets (avoid broadcasting large data)
      val filters = filtersToApply.map { joinColumn =>
        val storageColumn = columnMappings(joinColumn)
        val values = indexes(storageColumn)
        col(joinColumn).isin(values: _*)
      }
      readIndex.filter(filters.reduce(_ && _))
    } else {
      // Use broadcast join filtering for small value sets (efficient for small data)
      applyBroadcastJoinFiltering(readIndex, filtersToApply, columnMappings, indexes)
    }
  }

  /** Creates filter conditions for DataFrame joins based on a DataFrame of values.
    * Uses broadcast join approach to avoid collecting data to the driver.
    *
    * @param readIndex The DataFrame to filter
    * @param valuesDf DataFrame containing the distinct values to filter on
    * @param joinColumnsToUse The columns to create filters for
    * @return Filtered DataFrame
    */
  protected def applyJoinFiltersFromDataFrame(
    readIndex: DataFrame,
    valuesDf: DataFrame,
    joinColumnsToUse: Seq[String]
  ): DataFrame = {
    import org.apache.spark.sql.functions.broadcast
    
    if (joinColumnsToUse.isEmpty) {
      return readIndex
    }
    
    logger.warn(s"Applying broadcast join filtering from DataFrame with ${joinColumnsToUse.size} columns")
    
    // Use broadcast semi-join to filter the readIndex DataFrame
    // This is more efficient than collecting values and using isin()
    readIndex.join(broadcast(valuesDf), joinColumnsToUse, "leftsemi")
  }
  
  /** Applies filtering using broadcast join approach for large value sets.
    *
    * @param readIndex The DataFrame to filter
    * @param joinColumnsToUse The columns to filter on
    * @param columnMappings Mapping from join columns to storage columns
    * @param indexes The index values to filter on
    * @return Filtered DataFrame using broadcast join approach
    */
  protected def applyBroadcastJoinFiltering(
    readIndex: DataFrame,
    joinColumnsToUse: Seq[String],
    columnMappings: Map[String, String],
    indexes: Map[String, Array[Any]]
  ): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.functions.broadcast
    
    logger.warn(s"Using broadcast join filtering for large value sets with ${joinColumnsToUse.size} columns")
    
    // Apply broadcast joins for each filter column
    joinColumnsToUse.foldLeft(readIndex) { (accumDf, joinColumn) =>
      val storageColumn = columnMappings(joinColumn)
      val values = indexes(storageColumn)
      
      // Filter out null values to prevent NullPointerException
      val nonNullValues = values.filter(_ != null)
      logger.warn(s"Creating broadcast filter DataFrame for column $joinColumn with ${nonNullValues.length} non-null values (filtered from ${values.length} total)")
      
      if (nonNullValues.isEmpty) {
        // If all values are null, return empty result since null values won't match in joins
        logger.warn(s"All values for column $joinColumn are null, returning empty DataFrame")
        accumDf.filter(lit(false))
      } else {
        // Get the actual data type from the DataFrame schema
        val columnDataType = accumDf.schema.fields.find(_.name == joinColumn) match {
          case Some(field) => field.dataType
          case None => StringType // fallback to StringType
        }
        
        // Create DataFrame with the filter values using proper data type
        val schema = StructType(Array(StructField(joinColumn, columnDataType, nullable = false)))
        val rows = nonNullValues.map(v => Row(v))
        val filterDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
        
        // Use broadcast semi join to filter - more efficient for large sets by avoiding shuffle
        accumDf.join(broadcast(filterDf), Seq(joinColumn), "leftsemi")
      }
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
    // Validate that join columns are included in selected columns (if selection is active)
    // or exist in the schema (if no selection)
    getSelectedColumns match {
      case Some(selectedCols) =>
        val missingJoinCols = usingColumns.filterNot(selectedCols.contains)
        if (missingJoinCols.nonEmpty) {
          throw new ColumnNotFoundException(s"Join columns must be included in selected columns. Missing: ${missingJoinCols.mkString(", ")}")
        }
      case None =>
        // No selection active, but still validate columns exist in schema or are available indexes
        val invalidJoinCols = usingColumns.filterNot { colName =>
          SchemaHelper.fieldExists(storedSchema, colName) || this.indexes.contains(colName)
        }
        if (invalidJoinCols.nonEmpty) {
          throw new ColumnNotFoundException(s"Join columns not found in schema or indexes: ${invalidJoinCols.mkString(", ")}")
        }
    }
    
    // Map join columns to storage columns
    val columnMappings = mapJoinColumnsToStorage(usingColumns)

    // Include both regular storage columns and bloom storage columns
    val allStorageColumns = this.storageColumns ++ this.bloomStorageColumns
    val storageColumnsToUse = columnMappings.values.toSet.intersect(allStorageColumns)
    logger.warn(s"Found indexes for ${storageColumnsToUse.mkString(",")}")

    // Get values from the user DataFrame using join column names
    val joinColumnsToUse = usingColumns.filter(col =>
      columnMappings.contains(col) && storageColumnsToUse.contains(columnMappings(col))
    )
    
    // Get distinct values as a DataFrame (no collect to driver)
    val filteredValuesDf = df.select(joinColumnsToUse.map(col): _*).distinct()

    // Use the new DataFrame-based method to locate files (uses broadcast joins internally)
    val files = locateFilesFromDataFrame(filteredValuesDf, columnMappings, joinColumnsToUse)
    logger.warn(s"Found ${files.size} files in index")
    val readIndex = readFiles(files)

    // For bloom columns, we don't need to apply additional filters on the actual data
    // (the bloom filter already did the filtering at the file level)
    // For regular columns, apply filters using DataFrame-based approach
    val bloomColumnSet = bloomColumns
    val nonBloomJoinColumns = joinColumnsToUse.filterNot(bloomColumnSet.contains)
    
    val filteredReadIndex = if (nonBloomJoinColumns.nonEmpty) {
      applyJoinFiltersFromDataFrame(readIndex, filteredValuesDf, nonBloomJoinColumns)
    } else {
      readIndex
    }

    // Cache key must include join columns to avoid returning wrong cached results
    // We use a hash of the valuesDf to represent the actual values being joined
    val valuesDfHash = filteredValuesDf.queryExecution.analyzed.semanticHash()
    val cacheKey = (files, joinColumnsToUse.toSet, valuesDfHash)
    
    // Note: We can't use the old cache type since the key structure changed
    // Clear old cache entries and use new structure
    val typedCache = mutable.Map[(Set[String], Set[String], Int), DataFrame]()
    typedCache.getOrElseUpdate(cacheKey, filteredReadIndex.cache)
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