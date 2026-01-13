package com.xgen.mongot.index.lucene.quantization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Assert;
import org.junit.Test;

public class BinaryQuantizerTest {
  // CLOUDP-289670: work around Lucene ScalarQuantizer division-by-zero bug
  //     new BinaryQuantizer(0, 0);
  // This line causes a division-by-zero error inside Lucene's ScalarQuantizer until Lucene 11.
  // In Lucene 9/10, and with our BinaryQuantizer, the error is harmless and undetectable.
  // However, we still intentionally trigger the bug in this unit test in case the Lucene
  // code changes in some way that makes the bug more of a problem, such as by throwing.
  @Test
  public void testLuceneDivisionByZeroBug() {
    // Causes a division-by-zero error inside Lucene's ScalarQuantizer until Lucene 11.
    new BinaryQuantizer(0, 0);
  }

  // This unit test is to check the behavior of various IEEE 754 floating-point constants.
  @Test
  public void quantize_unusualFloats_matchesLtSemantics() {
    BinaryQuantizer quantizer = new BinaryQuantizer(1, 1);
    float[] input = {
      Float.NEGATIVE_INFINITY,
      Float.NaN,
      -Float.MAX_VALUE,
      -Float.MIN_VALUE,
      -0f,
      0f,
      Float.MIN_VALUE,
      Float.MAX_VALUE,
      Float.POSITIVE_INFINITY
    };
    byte[] output = new byte[input.length];

    quantizer.quantize(input, output, VectorSimilarityFunction.COSINE);

    Assert.assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 1, 1, 1}, output);
  }
}
