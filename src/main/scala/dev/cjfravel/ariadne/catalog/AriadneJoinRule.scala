package dev.cjfravel.ariadne.catalog

import dev.cjfravel.ariadne.Index
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{AriadneInternalHelper, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._

/** Catalyst optimizer rule that rewrites JOINs involving Ariadne tables.
  *
  * When a SQL query joins against an Ariadne catalog table, this rule intercepts
  * the logical plan and replaces the Ariadne scan with a pre-pruned read that
  * only includes files matching the join values. The original `Join` node,
  * condition, hints, and ExprIds are all preserved.
  *
  * '''Safety constraints (the rule only fires when ALL hold):'''
  *  1. The join type is `INNER`
  *  2. The entire condition is composed of equi-join predicates (`a.col = b.col`)
  *  3. Every Ariadne-side join column is indexed in the Ariadne index
  *  4. The Ariadne table is the direct child of the `Join` node (no
  *     intervening Filter, Project, or other operators)
  *
  * Column names may differ between sides (e.g., `c.id = q.customer_id`);
  * the rule identifies which attribute belongs to the Ariadne side by ExprId
  * and maps values accordingly for `locateFiles()`.
  *
  * When any constraint is not met, the rule does not fire and Spark falls back
  * to the standard V1Scan path (full table scan + standard join).
  *
  * '''How the rewrite works:'''
  *  1. Converts the non-Ariadne side into a DataFrame of join-key values
  *  2. Calls `locateFilesFromDataFrame()` which performs a distributed join
  *     against the Ariadne index table — value matching stays in Spark
  *     executors and only the final filename set (bounded by file count)
  *     is collected to the driver
  *  3. Replaces the Ariadne `DataSourceV2Relation` with a plan that reads only
  *     those files, aliased to preserve the original output ExprIds
  *  4. Applies temporal deduplication if the index has temporal columns
  *  5. Returns a new `Join` node with the pruned Ariadne side, preserving the
  *     original condition, join type, and hints
  *
  * '''Limitations:'''
  *  - Non-equi conditions, outer/semi/anti joins, and partially-indexed
  *    conditions all fall back to the V1Scan path
  *  - The other side of the join is executed during optimization to perform
  *    the distributed file lookup. This is consistent with the behavior
  *    of the programmatic `Index.join()` API.
  *
  * '''Registration:''' This rule is registered via [[AriadneSparkExtension]]:
  * {{{
  * // spark.sql.extensions must be set on SparkConf before SparkContext creation
  * conf.set("spark.sql.extensions",
  *   "dev.cjfravel.ariadne.catalog.AriadneSparkExtension")
  * }}}
  *
  * @param sparkSession the active SparkSession
  *
  * @see [[AriadneSparkExtension]] for registration
  * @see [[AriadneCatalog]] for the catalog integration
  */
class AriadneJoinRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private val logger = LogManager.getLogger("ariadne")

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case j @ Join(left, right, joinType, Some(condition), hint) =>
      val leftAriadne = extractDirectAriadneTable(left)
      val rightAriadne = extractDirectAriadneTable(right)

      (leftAriadne, rightAriadne) match {
        case (Some((tableName, _)), None) =>
          rewriteJoin(tableName, left, right, condition, joinType, hint,
            isAriadneLeft = true).getOrElse(j)

        case (None, Some((tableName, _))) =>
          rewriteJoin(tableName, right, left, condition, joinType, hint,
            isAriadneLeft = false).getOrElse(j)

        case _ => j
      }
  }

  /** Attempts to optimize a join by pre-pruning the Ariadne side's file list.
    *
    * Instead of replacing the entire Join, this method replaces only the Ariadne
    * scan with a pre-pruned read. The original Join node, condition, hints, and
    * ExprIds are preserved, avoiding ExprId mismatch errors.
    *
    * @param ariadneIndexName the Ariadne index name
    * @param ariadnePlan the logical plan of the Ariadne side
    * @param otherPlan the logical plan of the non-Ariadne side
    * @param condition the join condition expression
    * @param joinType the join type (must be Inner)
    * @param hint the join hint from the original plan
    * @param isAriadneLeft true if the Ariadne table is on the left side
    * @return Some(rewritten plan) if successful, None to fall back
    */
  private def rewriteJoin(
      ariadneIndexName: String,
      ariadnePlan: LogicalPlan,
      otherPlan: LogicalPlan,
      condition: Expression,
      joinType: org.apache.spark.sql.catalyst.plans.JoinType,
      hint: org.apache.spark.sql.catalyst.plans.logical.JoinHint,
      isAriadneLeft: Boolean
  ): Option[LogicalPlan] = {
    if (joinType != Inner) {
      logger.warn(
        s"AriadneJoinRule: only INNER joins are optimized for index " +
          s"'$ariadneIndexName', got ${joinType.sql}, skipping"
      )
      return None
    }

    if (!isAllEquiJoin(condition)) {
      logger.warn(
        s"AriadneJoinRule: condition contains non-equi predicates for index " +
          s"'$ariadneIndexName', skipping optimization"
      )
      return None
    }

    val equiPairs = extractEquiJoinPairs(condition)
    if (equiPairs.isEmpty) {
      logger.warn(
        s"AriadneJoinRule: no equi-join columns found for index " +
          s"'$ariadneIndexName', skipping optimization"
      )
      return None
    }

    // Identify which attribute in each pair belongs to the Ariadne side
    val ariadneExprIds = ariadnePlan.output.map(_.exprId).toSet
    val columnMapping = equiPairs.flatMap { case (left, right) =>
      if (ariadneExprIds.contains(left.exprId))
        Some((left.name, right.name)) // (ariadneCol, otherCol)
      else if (ariadneExprIds.contains(right.exprId))
        Some((right.name, left.name)) // (ariadneCol, otherCol)
      else
        None
    }
    if (columnMapping.length != equiPairs.length) {
      logger.warn(
        s"AriadneJoinRule: could not resolve column ownership for index " +
          s"'$ariadneIndexName', skipping optimization"
      )
      return None
    }

    val ariadneJoinCols = columnMapping.map(_._1).distinct

    implicit val spark: SparkSession = sparkSession
    val index = Index(ariadneIndexName)
    val allIndexed = allIndexedColumns(index)

    if (!ariadneJoinCols.forall(allIndexed.contains)) {
      logger.warn(
        s"AriadneJoinRule: not all Ariadne join columns " +
          s"[${ariadneJoinCols.mkString(", ")}] are indexed in " +
          s"'$ariadneIndexName', skipping optimization"
      )
      return None
    }

    logger.warn(
      s"AriadneJoinRule: rewriting INNER join on index '$ariadneIndexName' " +
        s"with columns [${columnMapping.map { case (a, o) => s"$a=$o" }.mkString(", ")}]"
    )

    try {
      val otherDf = AriadneInternalHelper.dataFrameFromPlan(spark, otherPlan)

      // Use locateFilesFromDataFrame for distributed file matching.
      // This keeps the heavy value-matching work distributed in Spark —
      // only the final filename set is collected to the driver, which is
      // bounded by file count (typically thousands), not join key cardinality
      // (potentially millions).
      val mappingMap = columnMapping.toMap // ariadneCol -> otherCol, deduped
      val valuesDf = otherDf.select(
        mappingMap.map { case (ariadneCol, otherCol) =>
          functions.col(otherCol).as(ariadneCol)
        }.toSeq: _*
      )
      val colMappings = ariadneJoinCols.map(c => c -> c).toMap
      val files = index.locateFilesFromDataFrame(valuesDf, colMappings, ariadneJoinCols)
      val originalOutput = ariadnePlan.output

      val prunedDf = if (files.nonEmpty) {
        val rawDf = index.readFiles(files)
        val temporalCols = index.metadata.temporal_indexes.asScala.map(_.column).toSeq
        val dedupedDf = index.applyTemporalDeduplication(rawDf, temporalCols)
        dedupedDf.select(originalOutput.map(a => functions.col(a.name)): _*)
      } else {
        val schema = StructType(originalOutput.map(a =>
          StructField(a.name, a.dataType, a.nullable)))
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }

      // Alias output to preserve original ExprIds so parent operators resolve
      val prunedPlan = prunedDf.queryExecution.analyzed
      val aliasedExprs = originalOutput.zip(prunedPlan.output).map {
        case (orig, newAttr) =>
          Alias(newAttr, orig.name)(exprId = orig.exprId)
      }
      val aliasedPlan = Project(aliasedExprs, prunedPlan)

      val (newLeft, newRight) = if (isAriadneLeft) {
        (aliasedPlan, otherPlan)
      } else {
        (otherPlan, aliasedPlan)
      }

      logger.warn(
        s"AriadneJoinRule: optimized join for index '$ariadneIndexName' " +
          s"(${files.size} file(s) located)"
      )
      Some(Join(newLeft, newRight, joinType, Some(condition), hint))
    } catch {
      case e: InterruptedException => throw e
      case e: Exception =>
        logger.warn(
          s"AriadneJoinRule: failed to optimize join for index " +
            s"'$ariadneIndexName': ${e.getMessage}"
        )
        None
    }
  }

  /** Returns true if the condition consists entirely of equi-join predicates. */
  private def isAllEquiJoin(condition: Expression): Boolean = condition match {
    case And(left, right)                    => isAllEquiJoin(left) && isAllEquiJoin(right)
    case EqualTo(_: Attribute, _: Attribute) => true
    case _                                   => false
  }

  /** Extracts (left, right) attribute pairs from equi-join conditions. */
  private def extractEquiJoinPairs(
      condition: Expression
  ): Seq[(Attribute, Attribute)] = condition match {
    case And(left, right) =>
      extractEquiJoinPairs(left) ++ extractEquiJoinPairs(right)
    case EqualTo(left: Attribute, right: Attribute) =>
      Seq((left, right))
    case _ => Seq.empty
  }

  /** Checks if the plan IS a direct AriadneTable reference (not nested).
    * Matches DataSourceV2Relation or DataSourceV2ScanRelation at the
    * top level. Also looks through column-pruning Projects (Projects whose
    * expressions are all simple Attributes) which Spark's ColumnPruning
    * rule inserts above the scan when not all columns are selected.
    * Does NOT look through Filters, Aggregates, or expression Projects
    * to avoid silently discarding operators that transform data.
    */
  private def extractDirectAriadneTable(
      plan: LogicalPlan
  ): Option[(String, AriadneTable)] = plan match {
    case DataSourceV2Relation(table: AriadneTable, _, _, _, _) =>
      Some((table.indexName, table))
    case DataSourceV2ScanRelation(
          DataSourceV2Relation(table: AriadneTable, _, _, _, _),
          _, _, _, _
        ) =>
      Some((table.indexName, table))
    case Project(projectList, child) if projectList.forall(_.isInstanceOf[Attribute]) =>
      extractDirectAriadneTable(child)
    case _ => None
  }

  private def allIndexedColumns(index: Index): Set[String] = {
    val md = index.metadata
    val regular = md.indexes.asScala.toSet
    val bloom = md.bloom_indexes.asScala.map(_.column).toSet
    val computed = md.computed_indexes.asScala.keys.toSet
    val temporal = md.temporal_indexes.asScala.map(_.column).toSet
    val range = md.range_indexes.asScala.map(_.column).toSet
    val exploded = md.exploded_field_indexes.asScala.map(_.as_column).toSet
    regular ++ bloom ++ computed ++ temporal ++ range ++ exploded
  }
}
