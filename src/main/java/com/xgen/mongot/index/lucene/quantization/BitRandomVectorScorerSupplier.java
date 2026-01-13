package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

public class BitRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
  protected final RandomAccessVectorValues.Bytes vectorValues;
  protected final RandomAccessVectorValues.Bytes vectorValues1;
  protected final RandomAccessVectorValues.Bytes vectorValues2;

  public BitRandomVectorScorerSupplier(RandomAccessVectorValues.Bytes vectorValues)
      throws IOException {
    this.vectorValues = vectorValues;
    this.vectorValues1 = vectorValues.copy();
    this.vectorValues2 = vectorValues.copy();
  }

  @Override
  public RandomVectorScorer scorer(int ord) throws IOException {
    byte[] query = this.vectorValues1.vectorValue(ord);
    return new BitRandomVectorScorer(this.vectorValues2, query);
  }

  @Override
  public RandomVectorScorerSupplier copy() throws IOException {
    return new BitRandomVectorScorerSupplier(this.vectorValues.copy());
  }
}
