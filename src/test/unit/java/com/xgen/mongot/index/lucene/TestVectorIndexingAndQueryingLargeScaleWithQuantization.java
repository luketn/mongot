package com.xgen.mongot.index.lucene;


import com.googlecode.junittoolbox.ParallelParameterized;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.bson.Vector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(ParallelParameterized.class)
public class TestVectorIndexingAndQueryingLargeScaleWithQuantization
    extends TestVectorIndexingAndQueryingLargeScale {
  public TestVectorIndexingAndQueryingLargeScaleWithQuantization(
      VectorSimilarity vectorSimilarity, boolean exact, int numPartitions) {
    super(vectorSimilarity, exact, numPartitions);
  }

  @Parameterized.Parameters
  public static Object[][] data() {
    return getTestParams();
  }

  @Test
  public void testScalarQuantization() throws Exception {
    super.runTest(Vector.VectorType.FLOAT, VectorQuantization.SCALAR);
  }

  @Test
  public void testBinaryQuantization() throws Exception {
    // Random vectors don't perform well with binary quantization.
    // Increasing num candidates helps with getting a reasonable recall value
    this.testHarness.setNumCandidatesAndLimit(1000, 10);
    // Also reducing the target recall for binary quantization test
    // as binary quantization does not perform well for random vectors
    this.testHarness.setTargetRecall(0.6);
    super.runTest(Vector.VectorType.FLOAT, VectorQuantization.BINARY);
  }
}
