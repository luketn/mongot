package com.xgen.mongot.index.lucene;


import com.googlecode.junittoolbox.ParallelParameterized;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.bson.Vector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(ParallelParameterized.class)
public class TestBitVectorIndexingAndQueryingLargeScale
    extends TestVectorIndexingAndQueryingLargeScale {

  public TestBitVectorIndexingAndQueryingLargeScale(
      VectorSimilarity vectorSimilarity, boolean exact, int numPartitions) {
    super(vectorSimilarity, exact, numPartitions);
  }

  @Parameterized.Parameters
  public static Object[][] data() {
    return getTestParams();
  }

  @Test
  public void runTest() throws Exception {
    // Bit vectors only support Euclidean similarity
    if (this.vectorSimilarity != VectorSimilarity.EUCLIDEAN) {
      return;
    }
    super.runTest(Vector.VectorType.BIT);
  }
}
