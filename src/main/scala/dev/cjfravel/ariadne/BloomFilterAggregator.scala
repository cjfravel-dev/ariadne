package dev.cjfravel.ariadne

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
 * Input row for [[BloomFilterAggregator]].
 *
 * `expectedInsertions` is carried on every row rather than supplied to the aggregator's constructor because a Spark
 * `Aggregator` cannot see its grouping key. Carrying the per-file distinct count in the row is what allows each file's
 * bloom filter to be sized individually; a constructor parameter would force one uniform size across every file in the
 * batch, inflating stored index size whenever file cardinalities are skewed.
 *
 * All rows within a single `filename` group carry the same `expectedInsertions`, which is what makes buffer merging
 * safe (see [[BloomFilterAggregator.merge]]).
 *
 * @param value
 *   the indexed value, already converted to its canonical string form
 * @param expectedInsertions
 *   the number of distinct values for the file this row belongs to
 */
private[ariadne] case class BloomAggInput(value: String, expectedInsertions: Long)

/**
 * Mutable aggregation buffer wrapping a Guava bloom filter.
 *
 * The filter starts as `null` because its size depends on `expectedInsertions`, which is only known once the first
 * input row arrives. A buffer that never received a row therefore stays empty and contributes nothing on merge.
 *
 * Not thread-safe; Spark confines each buffer instance to a single task.
 *
 * @param filter
 *   the underlying bloom filter, or `null` until the first value is added
 */
private[ariadne] class BloomFilterBuffer(var filter: BloomFilter[CharSequence]) extends Serializable

/**
 * Spark `Aggregator` that folds values into a Guava bloom filter incrementally.
 *
 * The buffer '''is''' the bloom filter: each value is hashed in and discarded immediately, so peak executor memory is
 * the filter's own size — roughly 1.2 bytes per distinct value at a 1% false positive rate — rather than the file's
 * distinct values held as boxed JVM objects. A bloom-indexed column is therefore unbounded in cardinality.
 *
 * Values must already be converted to the same canonical string form used at query time; see
 * `BloomFilterOperations.canonicalStringColumn`. Mismatched string forms would produce false negatives, which a bloom
 * filter is otherwise guaranteed never to yield.
 *
 * @param fpr
 *   desired false positive rate, strictly between 0 and 1
 * @throws IllegalArgumentException
 *   if `fpr` is outside the range (0, 1)
 */
private[ariadne] class BloomFilterAggregator(fpr: Double)
    extends Aggregator[BloomAggInput, BloomFilterBuffer, Array[Byte]] {
  require(fpr > 0.0 && fpr < 1.0, s"False positive rate must be between 0 and 1 (exclusive), got: $fpr")

  private val falsePositiveRate: Double = fpr

  /**
   * Returns an empty buffer whose filter is not yet allocated.
   *
   * @return
   *   an unsized [[BloomFilterBuffer]]
   */
  def zero: BloomFilterBuffer = new BloomFilterBuffer(null)

  /**
   * Adds a single value to the buffer, allocating the filter on first use.
   *
   * @param buffer
   *   the buffer to fold into
   * @param input
   *   the input row; null rows and null values are ignored
   * @return
   *   the same buffer instance, mutated
   */
  def reduce(buffer: BloomFilterBuffer, input: BloomAggInput): BloomFilterBuffer =
    if (input == null || input.value == null) {
      buffer
    } else {
      if (buffer.filter == null) {
        // Guava requires a positive expected insertion count; degrade gracefully if the
        // pre-count is missing or nonsensical rather than failing the whole update.
        val expected = math.max(input.expectedInsertions, 1L)
        buffer.filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), expected, falsePositiveRate)
      }
      buffer.filter.put(input.value)
      buffer
    }

  /**
   * Merges two partial buffers.
   *
   * Guava's `putAll` requires both filters to have identical bit-array length and hash count. That holds here because
   * every row within a `filename` group carries the same `expectedInsertions` and the aggregator uses a fixed `fpr`, so
   * any two non-empty buffers for the same group were created with identical parameters.
   *
   * @param b1
   *   first buffer
   * @param b2
   *   second buffer
   * @return
   *   a buffer holding the union of both filters
   */
  def merge(b1: BloomFilterBuffer, b2: BloomFilterBuffer): BloomFilterBuffer =
    if (b1.filter == null) b2
    else if (b2.filter == null) b1
    else {
      b1.filter.putAll(b2.filter)
      b1
    }

  /**
   * Serializes the accumulated filter using Guava's `writeTo` format.
   *
   * @param buffer
   *   the completed buffer
   * @return
   *   the serialized filter, or `null` if the group contained no non-null values
   */
  def finish(buffer: BloomFilterBuffer): Array[Byte] =
    if (buffer.filter == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream()
      try {
        buffer.filter.writeTo(baos)
        baos.toByteArray
      } finally {
        baos.close()
      }
    }

  /**
   * Encoder for the aggregation buffer.
   *
   * Java serialization is used because Guava's `BloomFilter` defines its own `writeReplace` form, which Kryo's
   * reflective serializer does not honour. Buffers are only serialized at shuffle boundaries, not per row, so the cost
   * is immaterial.
   *
   * @return
   *   a Java-serialization encoder for [[BloomFilterBuffer]]
   */
  def bufferEncoder: Encoder[BloomFilterBuffer] =
    Encoders.javaSerialization[BloomFilterBuffer]

  /**
   * Encoder for the serialized filter output.
   *
   * @return
   *   the binary encoder
   */
  def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
