package com.xgen.mongot.index.lucene.quantization;

import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

public class DequantizedVectorValues extends FloatVectorValues {

  private final QuantizedByteVectorValues quantizedVectorValues;
  private final VectorSimilarityFunction similarityFunction;

  /**
   * A reusable buffer that enables vectorValues() to skip array allocations. This is valid because
   * the contract of FloatVectorValues specifies that the caller is responsible for copying the
   * result of vectorValue() if it needs to reuse it.
   */
  private final float[] scratchBuffer;

  public DequantizedVectorValues(
      QuantizedByteVectorValues quantizedVectorValues,
      VectorSimilarityFunction similarityFunction) {
    this.quantizedVectorValues = quantizedVectorValues;
    this.similarityFunction = similarityFunction;
    this.scratchBuffer = new float[quantizedVectorValues.dimension()];
  }

  @Override
  public float[] vectorValue() throws IOException {
    byte[] packedBits = this.quantizedVectorValues.vectorValue();
    BinaryQuantizationUtils.dequantize(packedBits, this.scratchBuffer);
    return this.scratchBuffer;
  }

  @Override
  public int dimension() {
    return this.quantizedVectorValues.dimension();
  }

  @Override
  public int size() {
    return this.quantizedVectorValues.size();
  }

  @Override
  public int docID() {
    return this.quantizedVectorValues.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return this.quantizedVectorValues.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return this.quantizedVectorValues.advance(target);
  }

  @Override
  public VectorScorer scorer(float[] query) throws IOException {
    return new VectorScorer() {

      @Override
      public float score() throws IOException {
        return DequantizedVectorValues.this.similarityFunction.compare(
            query, iterator().vectorValue());
      }

      @Override
      public FloatVectorValues iterator() {
        return DequantizedVectorValues.this;
      }
    };
  }
}
