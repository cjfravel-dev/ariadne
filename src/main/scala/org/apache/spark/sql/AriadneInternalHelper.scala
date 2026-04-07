package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Bridge helper to access `private[sql]` Spark APIs from Ariadne.
  *
  * This follows the standard pattern used by Delta Lake and other Spark
  * connectors: placing a helper in the `org.apache.spark.sql` package to
  * access `private[sql]` members like `Dataset.ofRows`.
  *
  * @see [[dev.cjfravel.ariadne.catalog.AriadneJoinRule]] for the primary consumer
  */
object AriadneInternalHelper {

  /** Creates a DataFrame from a resolved LogicalPlan.
    *
    * Wraps `Dataset.ofRows()` which is `private[sql]`.
    *
    * @param spark the active SparkSession
    * @param plan a resolved (analyzed) logical plan
    * @return a DataFrame wrapping the plan
    */
  def dataFrameFromPlan(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, plan)
  }
}
