package dev.cjfravel.ariadne.catalog

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSessionExtensions

/** Spark session extension that registers the Ariadne JOIN optimization rule.
  *
  * When registered, this extension injects the [[AriadneJoinRule]] into
  * Spark's optimizer. The rule rewrites JOINs involving Ariadne catalog
  * tables to use the optimized file-pruning join path.
  *
  * '''Configuration:'''
  * {{{
  * spark.conf.set("spark.sql.extensions",
  *   "dev.cjfravel.ariadne.catalog.AriadneSparkExtension")
  * }}}
  *
  * '''Note:''' If your Spark session also uses Delta Lake SQL features,
  * append this extension to the existing `spark.sql.extensions` value
  * (comma-separated).
  *
  * @see [[AriadneJoinRule]] for the optimizer rule
  * @see [[AriadneCatalog]] for the catalog plugin
  */
class AriadneSparkExtension extends (SparkSessionExtensions => Unit) {

  private val logger = LogManager.getLogger("ariadne")

  override def apply(extensions: SparkSessionExtensions): Unit = {
    logger.warn("AriadneSparkExtension: registering AriadneJoinRule optimizer rule")
    extensions.injectOptimizerRule { session =>
      new AriadneJoinRule(session)
    }
  }
}
