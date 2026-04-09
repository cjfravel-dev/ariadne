package dev.cjfravel.ariadne.catalog

import dev.cjfravel.ariadne.{Index, IndexCatalog, IndexPathUtils}
import dev.cjfravel.ariadne.exceptions.IndexNotFoundException
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/** Spark V2 catalog plugin that exposes Ariadne indexes as SQL tables.
  *
  * All indexes under `spark.ariadne.storagePath` are automatically discovered
  * and exposed under a single `default` namespace. Users reference indexes as
  * `{catalogName}.{indexName}` (which resolves via the default namespace) or
  * explicitly as `{catalogName}.default.{indexName}`.
  *
  * '''Configuration:'''
  * {{{
  * spark.conf.set("spark.sql.catalog.ariadne", "dev.cjfravel.ariadne.catalog.AriadneCatalog")
  * }}}
  *
  * '''SQL usage:'''
  * {{{
  * SHOW TABLES IN ariadne;
  * DESCRIBE ariadne.customers;
  * SELECT * FROM ariadne.customers WHERE id = 123;
  * SELECT * FROM ariadne.customers c JOIN orders o ON c.id = o.customerid;
  * }}}
  *
  * This catalog is read-only. `createTable`, `dropTable`, and `alterTable`
  * throw `UnsupportedOperationException`. Index lifecycle management is
  * performed through the [[Index]] API.
  *
  * '''Thread safety:''' Spark creates one catalog instance per `SparkSession`.
  * The mutable `catalogName` field is set once during `initialize` and
  * read thereafter. Concurrent use across threads within a single session
  * is safe after initialization.
  *
  * @see [[AriadneTable]] for the table implementation
  * @see [[AriadneSparkExtension]] for registering the JOIN optimization rule
  */
class AriadneCatalog extends TableCatalog with SupportsNamespaces {

  private val logger = LogManager.getLogger("ariadne")
  private var catalogName: String = _
  private val defaultNs = Array("default")

  /** Initializes the catalog with the name assigned in the Spark configuration.
    *
    * Called once by Spark when the catalog is first accessed.
    *
    * @param name    the catalog name from `spark.sql.catalog.{name}`
    * @param options configuration options (currently unused)
    */
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    logger.warn(s"AriadneCatalog initialized with name '$name'")
  }

  /** Returns the catalog name assigned during initialization.
    *
    * @return the catalog name
    */
  override def name(): String = catalogName

  /** Returns `Array("default")` as the default namespace.
    *
    * This follows Spark conventions (Delta, Iceberg) so that
    * `ariadne.customers` resolves to `ariadne.default.customers` and
    * `SHOW TABLES IN ariadne` displays `default` in the namespace column.
    *
    * @return single-element array containing `"default"`
    */
  override def defaultNamespace(): Array[String] = defaultNs

  // --- TableCatalog ---

  /** Lists all Ariadne indexes as table identifiers.
    *
    * Delegates to [[IndexCatalog.list()]] which scans `storagePath/indexes/`
    * for directories containing `metadata.json`. All indexes are placed in
    * the `default` namespace.
    *
    * @param namespace must be `Array("default")` or empty
    * @return array of identifiers, one per index
    * @throws org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
    *   if an unrecognized namespace is provided
    */
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    validateNamespace(namespace)
    implicit val spark: SparkSession = SparkSession.active
    val names = IndexCatalog.list()
    logger.warn(s"AriadneCatalog.listTables: found ${names.size} index(es)")
    names.map(n => Identifier.of(defaultNs, n)).toArray
  }

  /** Loads an Ariadne index as a Spark V2 [[AriadneTable]].
    *
    * Reads the index's metadata to obtain the source data schema, format,
    * and index configuration.
    *
    * @param ident table identifier (namespace must be `default` or empty, name is the index name)
    * @return the AriadneTable wrapping this index
    * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException
    *   if no index with the given name exists
    */
  override def loadTable(ident: Identifier): Table = {
    implicit val spark: SparkSession = SparkSession.active
    val ns = ident.namespace()
    if (ns.nonEmpty && !ns.sameElements(defaultNs)) {
      throw new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(
        catalogName, ident.toString
      )
    }
    val indexName = ident.name()
    logger.warn(s"AriadneCatalog.loadTable: loading index '$indexName'")
    if (!IndexPathUtils.exists(indexName)) {
      throw new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(
        catalogName, indexName
      )
    }
    val index = Index(indexName)
    AriadneTable(indexName, index.storedSchema, index.metadata)
  }

  /** Checks whether an Ariadne index with the given name exists.
    *
    * @param ident table identifier (name is the index name)
    * @return true if the index exists on storage, false otherwise
    */
  override def tableExists(ident: Identifier): Boolean = {
    val ns = ident.namespace()
    if (ns.nonEmpty && !ns.sameElements(defaultNs)) return false
    implicit val spark: SparkSession = SparkSession.active
    IndexPathUtils.exists(ident.name())
  }

  /** Not supported — always throws.
    *
    * Use [[dev.cjfravel.ariadne.Index.apply]] to create indexes.
    *
    * @throws UnsupportedOperationException always
    */
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    throw new UnsupportedOperationException(
      "AriadneCatalog is read-only. Use Index.apply() to create indexes."
    )
  }

  /** Not supported — always throws.
    *
    * Use [[dev.cjfravel.ariadne.Index.remove]] to delete indexes.
    *
    * @throws UnsupportedOperationException always
    */
  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException(
      "AriadneCatalog is read-only. Use Index.remove() to delete indexes."
    )
  }

  /** Not supported — always throws.
    *
    * @throws UnsupportedOperationException always
    */
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException(
      "AriadneCatalog does not support renaming indexes."
    )
  }

  /** Not supported — always throws.
    *
    * Use the [[dev.cjfravel.ariadne.Index]] API to modify indexes.
    *
    * @throws UnsupportedOperationException always
    */
  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException(
      "AriadneCatalog is read-only. Use the Index API to modify indexes."
    )
  }

  // --- SupportsNamespaces (single "default" namespace) ---

  /** Lists available namespaces. Returns a single `"default"` namespace.
    *
    * @return array containing one element: `Array("default")`
    */
  override def listNamespaces(): Array[Array[String]] = {
    Array(defaultNs)
  }

  /** Lists child namespaces under the given namespace.
    *
    * Since Ariadne has a flat namespace structure, this always returns empty.
    *
    * @param namespace the parent namespace (must be `default` or empty)
    * @return empty array (no child namespaces)
    * @throws org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
    *   if the namespace is not `default`
    */
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    validateNamespace(namespace)
    Array.empty
  }

  /** Loads metadata for the given namespace.
    *
    * Returns an empty map for the `default` namespace. Ariadne does not
    * store namespace-level metadata.
    *
    * @param namespace the namespace to load (must be `default` or empty)
    * @return empty metadata map
    * @throws org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
    *   if the namespace is not `default`
    */
  override def loadNamespaceMetadata(
      namespace: Array[String]
  ): util.Map[String, String] = {
    validateNamespace(namespace)
    new util.HashMap[String, String]()
  }

  /** Not supported — always throws.
    *
    * Ariadne has a single `default` namespace.
    *
    * @throws UnsupportedOperationException always
    */
  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]
  ): Unit = {
    throw new UnsupportedOperationException(
      "AriadneCatalog has a single 'default' namespace. Additional namespaces are not supported."
    )
  }

  /** Not supported — always throws.
    *
    * Ariadne has a single `default` namespace.
    *
    * @throws UnsupportedOperationException always
    */
  override def alterNamespace(
      namespace: Array[String],
      changes: NamespaceChange*
  ): Unit = {
    throw new UnsupportedOperationException(
      "AriadneCatalog has a single 'default' namespace. Namespace alteration is not supported."
    )
  }

  /** Not supported — always throws.
    *
    * Ariadne has a single `default` namespace.
    *
    * @throws UnsupportedOperationException always
    */
  override def dropNamespace(
      namespace: Array[String],
      cascade: Boolean
  ): Boolean = {
    throw new UnsupportedOperationException(
      "AriadneCatalog has a single 'default' namespace. Namespace deletion is not supported."
    )
  }

  private def validateNamespace(namespace: Array[String]): Unit = {
    if (namespace.nonEmpty && !namespace.sameElements(defaultNs)) {
      throw new org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException(namespace)
    }
  }
}
