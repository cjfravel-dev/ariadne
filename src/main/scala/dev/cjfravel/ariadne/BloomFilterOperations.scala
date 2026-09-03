package dev.cjfravel.ariadne

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Trait providing bloom filter operations for [[Index]] instances.
 *
 * Bloom filters are probabilistic data structures used to test set membership. This trait handles the full lifecycle of
 * bloom filter indexes:
 *
 * '''Building''': During index updates, bloom filters are created per file per configured column via
 * [[buildBloomFilterIndexes]]. Values are hashed into a Guava `BloomFilter[CharSequence]` and serialized to
 * `Array[Byte]` for storage in the Delta index table.
 *
 * '''Querying''': At query time, [[locateFilesWithBloom]] and [[locateFilesWithBloomFromDataFrame]] deserialize stored
 * bloom filters and test candidate values, returning the set of files that might contain matches.
 *
 * '''Serialization''': Bloom filters are serialized/deserialized via Guava's `writeTo`/`readFrom` methods using a
 * `StringFunnel` with UTF-8 encoding. All indexed values are converted to strings before hashing.
 *
 * Bloom filters provide:
 *   - Guaranteed no false negatives (if filter says "no", value is definitely absent)
 *   - Configurable false positive rate (if filter says "yes", value might be present)
 *   - Space-efficient storage (approximately 10 bits per element at 1% FPR)
 *
 * Execution mechanics: filters are built by `BloomFilterAggregator` and probed by `bloomProbeUdf`, both of which run on
 * worker nodes. Neither ever collects filters or values to the driver, so memory scales with the size of a single
 * filter rather than with the data. The probe UDF captures only a local `Boolean` and the broadcast handle, so the
 * closure never drags `this` — and with it the non-serializable `SparkSession` — across the task boundary.
 *
 * @see
 *   [[BloomIndexConfig]] for per-column configuration
 * @see
 *   [[IndexBuildOperations]] for the build pipeline that invokes these operations
 */
trait BloomFilterOperations extends IndexFileOperations {
  self: Index =>

  /**
   * Column name prefix used when storing bloom filter binary data in the index Delta table.
   */
  protected val bloomColumnPrefix = "bloom_"

  /**
   * Internal column name holding the canonical string form of a value during bloom construction.
   */
  private val bloomValueColumn = "_ariadne_bloom_value"

  /**
   * Internal column name holding a file's distinct value count during bloom construction.
   */
  private val bloomExpectedColumn = "_ariadne_bloom_expected"

  /**
   * Returns the bloom index configurations from the current index metadata.
   *
   * @return
   *   sequence of [[BloomIndexConfig]] objects, one per bloom-indexed column
   */
  protected def bloomIndexConfigs: Seq[BloomIndexConfig] =
    metadata.bloom_indexes.asScala.toSeq

  /**
   * Returns the set of source column names that have bloom indexes configured.
   *
   * @return
   *   set of column names (without the `bloom_` prefix)
   */
  protected def bloomColumns: Set[String] =
    metadata.bloom_indexes.asScala.map(_.column).toSet

  /**
   * Returns the set of storage column names used for bloom filter binary data.
   *
   * Each name is the source column name prefixed with [[bloomColumnPrefix]].
   *
   * @return
   *   set of prefixed column names (e.g., `bloom_user_id`)
   */
  protected def bloomStorageColumns: Set[String] =
    metadata.bloom_indexes.asScala.map(c => bloomColumnPrefix + c.column).toSet

  /**
   * Builds bloom filter indexes for all configured bloom columns.
   *
   * For each [[BloomIndexConfig]], this method:
   *   1. Reduces the input to distinct (filename, value) pairs
   *   1. Computes each file's distinct value count so its filter can be sized individually
   *   1. Folds the values into a bloom filter with `BloomFilterAggregator`
   *   1. Joins the resulting bloom column back onto the accumulating DataFrame
   *
   * The aggregation is streaming: values are hashed into the filter and discarded, so executor memory scales with the
   * size of the filter (~1.2 bytes per distinct value at 1% FPR) rather than with the number of values held as boxed
   * JVM objects. A bloom-indexed column is therefore not bounded by what fits in an executor-side array.
   *
   * If no bloom indexes are configured, returns a distinct filename-only DataFrame.
   *
   * @param df
   *   DataFrame containing a `filename` column and all bloom-indexed source columns
   * @return
   *   DataFrame with `filename` plus one `bloom_{column}` binary column per configured bloom index
   */
  protected def buildBloomFilterIndexes(df: DataFrame): DataFrame = {
    val configs = bloomIndexConfigs
    if (configs.isEmpty) {
      df.select("filename").distinct()
    } else {
      logger.debug(
        s"Building bloom filter indexes for ${configs.size} columns: ${configs.map(_.column).mkString(", ")}")

      configs.foldLeft(df.select("filename").distinct) { (accumDf, config) =>
        val bloomColumn = bloomColumnPrefix + config.column
        val bloomData = buildStreamingBloomColumn(df, col(config.column), bloomColumn, config.fpr)
        accumDf.join(bloomData, Seq("filename"), "left")
      }
    }
  }

  /**
   * Folds a column of values into one serialized bloom filter per file.
   *
   * The aggregation is streaming: values are hashed into the filter and discarded, so executor memory scales with the
   * size of the filter (~1.2 bytes per distinct value at 1% FPR) rather than with the number of values held as boxed
   * JVM objects. A bloom-backed column is therefore not bounded by what fits in an executor-side array.
   *
   * Values are reduced to distinct `(filename, value)` pairs first, so each value is inserted exactly once.
   *
   * @param df
   *   DataFrame containing a `filename` column and the source values
   * @param valueColumn
   *   the column expression holding the values to insert
   * @param bloomColumn
   *   name to give the resulting binary filter column
   * @param fpr
   *   desired false positive rate, strictly between 0 and 1
   * @return
   *   DataFrame of `filename` plus the serialized filter column; files contributing no non-null values are absent
   */
  protected def buildStreamingBloomColumn(
      df: DataFrame,
      valueColumn: Column,
      bloomColumn: String,
      fpr: Double): DataFrame = {
    val distinctValues =
      df
        .select(col("filename"), canonicalStringColumn(valueColumn).alias(bloomValueColumn))
        .where(col(bloomValueColumn).isNotNull)
        .distinct()

    // Per-file counts size each filter individually. This is small (one row per file), so
    // broadcasting it keeps the join from adding a second shuffle of the value rows.
    val expectedPerFile =
      distinctValues
        .groupBy("filename")
        .agg(count(lit(1)).alias(bloomExpectedColumn))

    val bloomAgg = udaf(new BloomFilterAggregator(fpr))
    distinctValues
      .join(broadcast(expectedPerFile), Seq("filename"), "inner")
      .groupBy("filename")
      .agg(bloomAgg(col(bloomValueColumn), col(bloomExpectedColumn)).alias(bloomColumn))
  }

  /**
   * Converts a column of any type to the canonical string form used for bloom hashing.
   *
   * Build and query sides must agree exactly on this representation: a bloom filter guarantees no false negatives, but
   * only if the same value hashes to the same string in both places. The query side stringifies driver-side JVM objects
   * with `toString`, so this wraps the value in a single-element array and applies `toString` inside a UDF. Spark
   * converts array elements to the same external JVM types (`java.sql.Timestamp`, `java.math.BigDecimal`, and so on)
   * that the driver sees, making the two paths byte-identical for '''scalar''' types.
   *
   * This does '''not''' hold for types whose `toString` is not value-based. `Array[Byte].toString` is an identity hash,
   * so two equal binary values stringify differently on the build and query sides and every probe misses. Binary
   * columns are therefore rejected by [[Index.addBloomIndex]] and excluded from auto-bloom. Nested array and map types
   * are untested and carry the same risk.
   *
   * A plain `cast(StringType)` would '''not''' be safe here — Spark renders a timestamp as `2024-01-01 00:00:00` while
   * `java.sql.Timestamp.toString` renders `2024-01-01 00:00:00.0`, which would silently produce false negatives.
   *
   * @param column
   *   the source column, of any type
   * @return
   *   a string column holding the canonical form, `null` where the source is `null`
   */
  protected def canonicalStringColumn(column: Column): Column = {
    val toCanonicalString =
      udf { (values: Seq[Any]) =>
        if (values == null || values.isEmpty || values.head == null) null
        else values.head.toString
      }
    toCanonicalString(array(column))
  }

  /**
   * Creates a Spark UDF that probes a serialized bloom filter against a broadcast set of candidate values.
   *
   * The returned UDF is safe to ship to executors: it closes over only the broadcast handle and a `Boolean`, never over
   * the enclosing [[Index]] (which holds a non-serializable `SparkSession`). The bloom filter is deserialized '''once
   * per row''' and then probed against every candidate value, rather than once per (row, value) pair.
   *
   * @param valuesBroadcast
   *   broadcast array of candidate values, already converted to `String` to match the representation used at build time
   *   by [[canonicalStringColumn]]
   * @param includeNullFilters
   *   result to return for rows whose bloom filter is `null`. Pass `true` where a missing filter must be treated as
   *   "might match" (e.g. auto-bloom backward compatibility with indexes built before auto-bloom existed), `false`
   *   where a missing filter means "no values".
   * @return
   *   UDF with signature `Array[Byte] => Boolean`
   */
  private def bloomProbeUdf(
      valuesBroadcast: Broadcast[Array[String]],
      includeNullFilters: Boolean): UserDefinedFunction = {
    // Capture only serializable locals; referencing trait members here would drag `this` (and the
    // SparkSession) into the closure and fail task serialization.
    val includeNulls = includeNullFilters
    udf { (bloomBytes: Array[Byte]) =>
      if (bloomBytes == null) includeNulls
      else {
        val bais = new ByteArrayInputStream(bloomBytes)
        try {
          val bf = BloomFilter.readFrom(bais, Funnels.stringFunnel(StandardCharsets.UTF_8))
          valuesBroadcast.value.exists(bf.mightContain)
        } finally {
          bais.close()
        }
      }
    }
  }

  /**
   * Probes every file's bloom filter for `bloomColumn` against `values`, entirely on the executors.
   *
   * This is the memory-safe counterpart to collecting bloom filter binary data to the driver. The candidate values are
   * broadcast, the `mightContain` test runs as a distributed filter, and '''only the surviving filenames''' are
   * returned to the driver. Driver memory is therefore bounded by the number of matching files rather than by the total
   * serialized size of the bloom filters.
   *
   * This distinction matters most for auto-bloom columns. Once any one file crosses `largeIndexLimit` distinct values
   * (500,000 by default), '''every''' file in that column gets a filter, each sized from that file's own distinct
   * count; the largest can reach roughly 585 KiB at the default 1% FPR. Collecting one per file would scale driver
   * memory with the number of indexed files.
   *
   * @param indexDf
   *   the index DataFrame containing `filename` and the bloom binary column
   * @param bloomColumn
   *   the fully prefixed storage column name (e.g. `bloom_user_id` or `auto_bloom_user_id`)
   * @param values
   *   candidate values to probe; nulls are ignored and the remainder de-duplicated before broadcast
   * @param includeNullFilters
   *   whether rows with a `null` bloom filter should be treated as candidates
   * @return
   *   set of filenames whose bloom filter indicates a possible match
   */
  protected def probeBloomFilters(
      indexDf: DataFrame,
      bloomColumn: String,
      values: Array[Any],
      includeNullFilters: Boolean): Set[String] = {
    val probeValues = values.filter(_ != null).map(_.toString).distinct
    val valuesBroadcast = spark.sparkContext.broadcast(probeValues)
    try {
      val probeUdf = bloomProbeUdf(valuesBroadcast, includeNullFilters)
      indexDf
        .select(col("filename"), col(bloomColumn))
        .where(probeUdf(col(bloomColumn)))
        .select("filename")
        .distinct()
        .collect()
        .map(_.getString(0))
        .filter(_ != null)
        .toSet
    } finally {
      safeDestroyBroadcast(valuesBroadcast)
    }
  }

  /**
   * Locates files that might contain any of the given values using bloom filters.
   *
   * The probe runs as a distributed Spark filter via [[probeBloomFilters]]: candidate values are broadcast to the
   * executors and only matching filenames are returned to the driver. Bloom filter binary data is never collected.
   *
   * @param column
   *   the source column name (without `bloom_` prefix)
   * @param values
   *   array of candidate values to search for
   * @param indexDf
   *   the index DataFrame containing bloom filter binary columns
   * @return
   *   set of filenames whose bloom filters indicate a possible match; empty set if the bloom column does not exist in
   *   the index
   */
  protected def locateFilesWithBloom(column: String, values: Array[Any], indexDf: DataFrame): Set[String] = {
    require(column != null && column.trim.nonEmpty, "column must not be null or blank")
    require(values != null, "values must not be null")
    require(indexDf != null, "indexDf must not be null")
    val bloomColumn = bloomColumnPrefix + column
    logger.warn(s"Querying bloom filter for column '$column' with ${values.length} values")

    if (!indexDf.columns.contains(bloomColumn)) {
      logger.warn(s"Bloom column $bloomColumn not found in index")
      Set.empty
    } else {
      // A null bloom filter for an explicit bloom index means the file held no values for this
      // column, so it cannot match.
      val matchingFiles = probeBloomFilters(indexDf, bloomColumn, values, includeNullFilters = false)
      logger.warn(s"Bloom filter for '$column': ${matchingFiles.size} files matched")
      matchingFiles
    }
  }

  /**
   * Locates files that might contain values from a DataFrame using bloom filters.
   *
   * Collects distinct non-null values from the specified column of `valuesDf`, then delegates to
   * [[locateFilesWithBloom]] for the actual bloom filter check.
   *
   * @note
   *   This method collects the distinct candidate values from `valuesDf` to the driver so they can be broadcast for the
   *   probe. Bloom filter binary data is '''not''' collected — see [[probeBloomFilters]]. Driver memory is therefore
   *   bounded by the join-key cardinality of `valuesDf`. This value set is never '''truncated''': dropping probe values
   *   would prune away files that hold them and silently omit matching rows. When the probe bound is exceeded the
   *   pre-filter is '''skipped''' instead, returning every indexed file — a safe superset that the subsequent read and
   *   join narrow correctly.
   *
   * @param column
   *   the source column name (without `bloom_` prefix)
   * @param valuesDf
   *   DataFrame containing the candidate values to search for
   * @param indexDf
   *   the index DataFrame containing bloom filter binary columns
   * @return
   *   set of filenames whose bloom filters indicate a possible match; empty set if no non-null values exist or the
   *   bloom column is missing
   */
  protected def locateFilesWithBloomFromDataFrame(
      column: String,
      valuesDf: DataFrame,
      indexDf: DataFrame): Set[String] = {
    require(column != null && column.trim.nonEmpty, "column must not be null or blank")
    require(valuesDf != null, "valuesDf must not be null")
    require(indexDf != null, "indexDf must not be null")
    val bloomColumn = bloomColumnPrefix + column

    if (!indexDf.columns.contains(bloomColumn)) {
      logger.warn(s"Bloom column $bloomColumn not found in index")
      Set.empty
    } else {
      val fpr = bloomIndexConfigs.find(_.column == column).map(_.fpr).getOrElse(0.01)
      collectProbeValues(valuesDf, column, fpr) match {
        case None =>
          // Over the bound the pre-filter cannot prune meaningfully. Returning every
          // indexed file is a superset, and the subsequent read and join filter to the
          // correct rows, so this costs I/O rather than correctness.
          logger.warn(
            s"Bloom pre-filter skipped for '$column': more than ${BloomFilterOperations.maxProbeValues(fpr)} " +
              s"distinct query values, past which a filter at FPR $fpr prunes less than " +
              s"${BloomFilterOperations.MinPruningFraction} of non-matching files. Results are unaffected.")
          allIndexedFiles(indexDf)

        case Some(values) if values.isEmpty => Set.empty

        case Some(values) =>
          logger.warn(s"Bloom filter query for '$column': ${values.length} distinct values from DataFrame")
          locateFilesWithBloom(column, values, indexDf)
      }
    }
  }

  /**
   * Every filename present in the index.
   *
   * Used as the result of a skipped bloom pre-filter: a superset of the true match set, which the subsequent read and
   * join narrow correctly.
   *
   * @param indexDf
   *   the main index DataFrame
   * @return
   *   set of all indexed filenames
   */
  private def allIndexedFiles(indexDf: DataFrame): Set[String] =
    indexDf.select("filename").distinct().collect().map(_.getString(0)).filter(_ != null).toSet

  /**
   * Collects the distinct non-null values of `column` from `valuesDf` for broadcasting, subject to the probe bound
   * derived from `fpr`.
   *
   * This is the single query-side entry point for '''both''' explicit bloom indexes and auto-bloom pre-filters. Both
   * collect query values to the driver and broadcast them, so both carry the same driver-memory exposure and are
   * bounded identically here; leaving either unbounded is a driver OOM waiting on a wide enough join key.
   *
   * @note
   *   Returns `None` rather than a truncated array when the bound is exceeded. Truncating would drop probe values and
   *   prune away the files holding them, turning a pre-filter into a source of missing rows. Skipping only widens the
   *   candidate set.
   *
   * @param valuesDf
   *   DataFrame supplying the query-side values
   * @param column
   *   the column of `valuesDf` holding the join keys
   * @param fpr
   *   false positive rate of the filters that will be probed, used to derive the bound
   * @return
   *   `Some(distinct values)` when within the bound, `None` when the pre-filter should be skipped
   */
  private[ariadne] def collectProbeValues(valuesDf: DataFrame, column: String, fpr: Double): Option[Array[Any]] = {
    require(valuesDf != null, "valuesDf must not be null")
    require(column != null && column.trim.nonEmpty, "column must not be null or blank")

    val bound = BloomFilterOperations.maxProbeValues(fpr)
    // Collect one row beyond the bound in a single action: enough to distinguish "within
    // bound" from "over bound" without a separate count job, and without pulling an
    // unbounded number of values to the driver. The extra row is a probe, never a value.
    val bounded =
      valuesDf
        .select(column)
        .where(col(column).isNotNull)
        .distinct()
        .limit(bound + 1)
        .collect()

    if (bounded.length > bound) None else Some(bounded.map(_.get(0)))
  }
}

/**
 * Derivation of the query-side probe bound shared by every bloom filter path.
 *
 * A bloom pre-filter only earns its keep while it still prunes files. A file that holds '''none''' of the probe values
 * survives the probe with probability `1 - (1 - fpr)^n`, so the fraction of non-matching files actually pruned is
 * `(1 - fpr)^n` and decays geometrically as the probe set grows. At the default 1% FPR that fraction is 90% at 10 probe
 * values, 37% at 100, 4.9% at 300, and 0.004% at 1000 — past roughly `ln(MinPruningFraction) / ln(1 - fpr)` values (458
 * at the default 1% FPR) the pre-filter prunes less than [[MinPruningFraction]] of non-matching files while still
 * requiring every distinct value to be collected to the driver and broadcast cluster-wide.
 *
 * The useful ceiling is therefore a property of the filter, not a user preference, and is derived here rather than
 * configured. Exposing it as a setting invites tuning it into the regime where it cannot help.
 *
 * @note
 *   Past the bound the pre-filter is '''skipped''', never truncated. Skipping widens the candidate file set, and the
 *   subsequent read and join still filter to the correct rows, so results are unaffected. Truncating would drop probe
 *   values and prune away files that hold them, silently losing rows.
 */
private[ariadne] object BloomFilterOperations {

  /**
   * The smallest fraction of non-matching files a pre-filter must still prune to be worth running.
   *
   * Below this the collect and broadcast cost is paid for no measurable reduction in files read.
   */
  val MinPruningFraction: Double = 0.01

  /**
   * Hard ceiling on probe values regardless of FPR.
   *
   * The derivation grows without bound as `fpr` approaches 0, so an extreme FPR would otherwise authorize collecting an
   * effectively unbounded value set to the driver. This is a driver-safety backstop, not a tuning parameter.
   */
  val AbsoluteMaxProbeValues: Int = 1000000

  /**
   * Largest probe set for which a filter with the given false positive rate still prunes at least
   * [[MinPruningFraction]] of non-matching files.
   *
   * Solves `(1 - fpr)^n >= MinPruningFraction` for `n`.
   *
   * @param fpr
   *   the false positive rate the filter was built with, in `(0, 1)`
   * @return
   *   the probe-set size bound, at least 1 and never above [[AbsoluteMaxProbeValues]]
   */
  def maxProbeValues(fpr: Double): Int = {
    require(fpr > 0.0 && fpr < 1.0, s"fpr must be in (0, 1), got: $fpr")
    val exact = math.log(MinPruningFraction) / math.log(1.0 - fpr)
    if (exact >= AbsoluteMaxProbeValues.toDouble) AbsoluteMaxProbeValues
    else math.max(1, math.floor(exact).toInt)
  }
}
