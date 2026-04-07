package dev.cjfravel.ariadne.catalog

import dev.cjfravel.ariadne.IndexMetadata
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/** Spark V2 table representing an Ariadne index's source data.
  *
  * When queried, this table reads the original data files that were indexed
  * (not the index metadata tables). The schema is the source data schema
  * stored in the index's `metadata.json`.
  *
  * This table supports batch reads only and is read-only. Write operations
  * are not supported — index updates are performed via [[dev.cjfravel.ariadne.Index.update]].
  *
  * When used in a JOIN, the [[AriadneJoinRule]] optimizer rule rewrites the
  * plan to use Ariadne's optimized file-pruning join path.
  *
  * @param indexName the name of the Ariadne index
  * @param sourceSchema the schema of the original source data files
  * @param metadata the index metadata containing format, indexes, and read options
  *
  * @see [[AriadneCatalog]] for catalog-level operations
  * @see [[AriadneScanBuilder]] for the scan implementation
  */
case class AriadneTable(
    indexName: String,
    sourceSchema: StructType,
    metadata: IndexMetadata
) extends Table
    with SupportsRead {

  override def name(): String = indexName

  override def schema(): StructType = sourceSchema

  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability]()
    caps.add(TableCapability.BATCH_READ)
    caps
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new AriadneScanBuilder(indexName, sourceSchema, metadata)
  }
}
