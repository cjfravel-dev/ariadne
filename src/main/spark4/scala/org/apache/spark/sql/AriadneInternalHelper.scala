package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

/**
 * Bridge helper to access Spark's internal plan-to-DataFrame factory from Ariadne.
 *
 * On the Spark 4 line the eager implementation lives in `org.apache.spark.sql.classic`, so this wraps
 * `org.apache.spark.sql.classic.Dataset.ofRows`. Placing the helper in the `org.apache.spark.sql` package mirrors the
 * standard pattern used by Delta Lake and other Spark connectors.
 *
 * @see
 *   [[dev.cjfravel.ariadne.catalog.AriadneJoinRule]] for the primary consumer
 */
object AriadneInternalHelper {

  /**
   * Creates a DataFrame from a resolved LogicalPlan.
   *
   * Wraps `classic.Dataset.ofRows()`.
   *
   * @param spark
   *   the active SparkSession
   * @param plan
   *   a resolved (analyzed) logical plan
   * @return
   *   a DataFrame wrapping the plan
   */
  def dataFrameFromPlan(spark: SparkSession, plan: LogicalPlan): DataFrame =
    ClassicDataset.ofRows(spark.asInstanceOf[ClassicSparkSession], plan)
}
