package dev.cjfravel.ariadne.exceptions

/**
 * Thrown when a join type is requested that cannot be answered meaningfully through an index.
 *
 * An Ariadne join returns the '''minimal complete dataset''' for the join: an indexed value is never missed, and
 * index-side rows that are not present in the joined DataFrame are deliberately discarded. Because the index locates
 * whole files rather than individual rows, the set of unmatched index-side rows that survive pruning is an artifact of
 * file layout and changes after compaction.
 *
 * Join types whose result is defined by those unmatched index-side rows therefore have no stable meaning, and are
 * rejected rather than silently returning a file-layout-dependent answer. Which types those are depends on which side
 * the index is on:
 *
 *   - [[dev.cjfravel.ariadne.Index.join]] places the index on the '''left''', so `left`, `full` and `left_anti` are
 *     rejected.
 *   - [[dev.cjfravel.ariadne.Index.DataFrameOps.join]] places the index on the '''right''', so `right` and `full` are
 *     rejected.
 *
 * '''Recovery:''' Use a join type that only depends on matched rows — `inner`, `cross`, or `left_semi` — or the outer
 * type that preserves the DataFrame side (`right` via `index.join`, `left` via `df.join(index, ...)`). To join against
 * every row of the indexed dataset regardless of matches, read the files directly instead of going through the index.
 *
 * '''Thread safety:''' Instances are immutable after construction and safe to share across threads.
 *
 * {{{
 * try {
 *   index.join(lookupDf, Seq("customer_id"), "left_anti")
 * } catch {
 *   case e: UnsupportedJoinTypeException =>
 *     // left_anti asks for exactly the rows the index discards
 *     logger.error(e.getMessage)
 * }
 * }}}
 *
 * @param message
 *   A descriptive error message naming the rejected join type and the supported alternatives
 */
class UnsupportedJoinTypeException(message: String) extends AriadneException(message)

/**
 * Factory for [[UnsupportedJoinTypeException]] instances.
 */
object UnsupportedJoinTypeException {

  /**
   * Creates an [[UnsupportedJoinTypeException]] describing why the join type cannot be served from an index.
   *
   * @param joinType
   *   the join type the caller requested, as written
   * @param indexSide
   *   the side the index-located data occupies in this call, `"left"` or `"right"`
   * @param supported
   *   the join types that are meaningful for that side
   * @return
   *   a new `UnsupportedJoinTypeException`
   */
  def apply(joinType: String, indexSide: String, supported: Seq[String]): UnsupportedJoinTypeException =
    new UnsupportedJoinTypeException(
      s"Join type '$joinType' is not supported for index-backed joins because the index-located data is on the " +
        s"$indexSide side, and this join type returns index rows that have no match in the other DataFrame. " +
        s"An index only guarantees that matching values are never missed; unmatched index rows are pruned at file " +
        s"granularity, so the result would depend on file layout and change after compaction. " +
        s"Supported join types here: ${supported.mkString(", ")}. " +
        s"To join against every row of the indexed dataset, read the data files directly instead of using the index.")
}
