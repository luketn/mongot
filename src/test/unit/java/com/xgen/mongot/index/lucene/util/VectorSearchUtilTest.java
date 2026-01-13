package com.xgen.mongot.index.lucene.util;

import static org.junit.Assert.assertEquals;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import org.junit.Assert;
import org.junit.Test;

public class VectorSearchUtilTest {

  @Test
  public void bitSimilarity_vectorizedNoPadding_returnsNormalizedMatches() {
    byte[] a = {(byte) 0xFF, 0x00, 0x7A, 0x78};
    byte[] b = {(byte) 0xF7, 0x71, 0x7A, 0x38};

    float result = VectorSearchUtil.bitSimilarity(a, b, a.length * 8);

    // 24 bits match out of 32
    assertEquals(0.8125, result, 0.0001);
  }

  @Test
  public void bitSimilarity_oddLength_returnsNormalizedMatches() {
    byte[] a = {0x40, 0x7A, 0x78}; // Too small to be vectorized
    byte[] b = {0x51, 0x7A, 0x38};

    float result = VectorSearchUtil.bitSimilarity(a, b, a.length * 8);

    // 21 bits match out of 24
    assertEquals(0.875, result, 0.0001);
  }

  @Test
  public void bitSimilarity_withPadding_assumesPaddingMatches() {
    byte[] a = {0x40, 0x7A, 0x78};
    byte[] b = {0x51, 0x7A, 0x38};
    int padding = 4;

    float result = VectorSearchUtil.bitSimilarity(a, b, a.length * 8 - padding);

    // 21 bits match out of 24, but 4 matches are ignored due to padding so 17/20
    assertEquals(0.85, result, 0.0001);
  }

  @Test
  public void bitSimilarity_isMonotonicInMatchedBits() {
    // This test ensures that computing bit similarity with float32 division doesn't harm recall.
    // We can compute similarity more precisely, but this is only an issue if dim > 8192*512
    // See comment in BitRandomVectorScorer to see why we need to multiply MAX_DIMENSIONS by 8
    int maxDimension = VectorFieldSpecification.MAX_DIMENSIONS * Byte.SIZE;

    byte[] allZeros = new byte[VectorFieldSpecification.MAX_DIMENSIONS];
    byte[] v = allZeros.clone();

    @Var float lastScore = 1.01f;
    for (int i = 0; i <= maxDimension; ++i) {
      float result = VectorSearchUtil.bitSimilarity(allZeros, v, maxDimension);

      if (result < 0 || result >= lastScore) {
        Assert.fail(
            String.format(
                "For i=%d, result should be in range [0, %f) but was %f", i, lastScore, result));
      }
      lastScore = result;

      if (i < maxDimension) {
        v[i / 8] |= (byte) (1 << (i % 8));
      }
    }
  }
}
