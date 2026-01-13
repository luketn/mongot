package com.xgen.mongot.index.lucene.quantization;

import com.xgen.testing.TestUtils;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class PackingUnpackingTest {

  private static final int HOW_MANY_VECTORS_TO_TEST = 100;

  @Test
  public void compressBytes_lessThanOneByte_addsZeroPadding() {
    // A trivial hardcoded test vector.
    // Note that one bit of padding is implied by the length of 7.
    byte[] originalVector = {1, 1, 0, 1, 0, 1, 0};

    // Compress the test vector.
    byte[] compressedVector =
        new byte[BinaryQuantizationUtils.requiredBytes(originalVector.length)];
    BinaryQuantizationUtils.compressBytes(originalVector, compressedVector);

    // Confirm that the compressed vector matches the hardcoded expected vector.
    // (Obtained from a two's complement programmer's calculator.)
    byte[] expectedCompressedVector = {-44};
    Assert.assertArrayEquals(compressedVector, expectedCompressedVector);
  }

  @Test
  public void dequantized_tooFewFloats_dropsRightPadding() {
    // A trivial hardcoded compressed test vector.
    // (Obtained from a two's complement programmer's calculator.)
    // Note that one bit of padding is implied by the length of 7.
    byte[] compressedVector = {-44};
    float[] dequantized = new float[7];

    // Decompress
    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    // Confirm that the decompressed vector matches the hardcoded expected vector.
    float[] expected = {1f, 1f, -1f, 1f, -1f, 1f, -1f};
    Assert.assertArrayEquals(expected, dequantized, TestUtils.EPSILON);
  }

  @Test
  public void testRandomVectorsOfAssortedSizes() {
    Random random = new Random();

    // Dimensions 1 through 16.
    for (int dimensions = 1; dimensions <= 16; dimensions++) {
      for (int count = 0; count < HOW_MANY_VECTORS_TO_TEST; ++count) {
        doTestForDimensions(random, dimensions);
      }
    }

    // Dimensions 256.
    for (int count = 0; count < HOW_MANY_VECTORS_TO_TEST; ++count) {
      doTestForDimensions(random, /* dimensions= */ 256);
    }

    // Dimensions 1024.
    for (int count = 0; count < HOW_MANY_VECTORS_TO_TEST; ++count) {
      doTestForDimensions(random, /* dimensions= */ 1024);
    }
  }

  private void doTestForDimensions(Random random, int dimensions) {

    // Generate random test vector.
    // No need to normalize the vector length because we aren't querying.
    byte[] originalVector = new byte[dimensions];
    float[] expected = new float[dimensions];
    for (int i = 0; i < originalVector.length; i++) {
      originalVector[i] = (byte) random.nextInt(1);
      expected[i] = originalVector[i] == 0 ? -1f : 1f;
    }
    float[] bufferToDequantize = new float[dimensions];

    // Compress the test vector.
    byte[] compressedVector =
        new byte[BinaryQuantizationUtils.requiredBytes(originalVector.length)];
    BinaryQuantizationUtils.compressBytes(originalVector, compressedVector);

    // Decompress
    BinaryQuantizationUtils.dequantize(compressedVector, bufferToDequantize);

    // Confirm that the decompressed vector matches the original generated vector.
    Assert.assertArrayEquals(expected, bufferToDequantize, TestUtils.EPSILON);
  }
}
