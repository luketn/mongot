package com.xgen.mongot.index.lucene.util;

import org.apache.lucene.util.VectorUtil;

/** Utility class for methods used in both vector indexing and search. */
public class VectorSearchUtil {

  private VectorSearchUtil() {
    // Do not instantiate
  }

  /**
   * Returns a similarity score of all bits in {@code x} and {@code y}
   *
   * <p>The score is computed as the negated Hamming distance scaled to the range [0, 1] where a
   * higher score implies a better match. This is equivalent to Euclidean distance for bit vectors,
   * but faster to compute.
   *
   * @param x - a bit vector densely packed into bytes, with zero padding
   * @param y - a bit vector densely packed into bytes, with zero padding
   * @param dim - the number of bits in the vectors (ignoring any padding)
   * @return a similarity score normalized to the range [0, 1]
   */
  public static float bitSimilarity(byte[] x, byte[] y, int dim) {
    return (dim - VectorUtil.xorBitCount(x, y)) / (float) dim;
  }
}
