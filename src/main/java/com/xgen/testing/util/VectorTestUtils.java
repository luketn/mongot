package com.xgen.testing.util;

import java.io.IOException;
import java.util.random.RandomGenerator;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

public class VectorTestUtils {

  /** Create a random vector with floats in range [-1, 1]. */
  public static float[] createFloatVector(int dim) {
    float[] v = new float[dim];
    RandomGenerator rng = RandomGenerator.getDefault();

    for (int i = 0; i < v.length; ++i) {
      v[i] = rng.nextFloat(-1, 1);
    }

    return v;
  }

  /** Create a random unit vector. */
  public static float[] createUnitVector(int dim) {
    return VectorUtil.l2normalize(createFloatVector(dim));
  }

  /**
   * Gets the QuantizedByteVectorValues associated with a vector field.
   *
   * @throws IOException if the field does not exist or is not quantized.
   */
  public static QuantizedByteVectorValues getQuantizedReader(
      LeafReader reader, String luceneFieldName) throws IOException {
    // QuantizedByteVectorValues can't be instantiated other than by a CodecReader, so this
    // repeated casting is actually how Lucene tests QuantizedByteVectorValue classes internally.
    // This will break in future version of Lucene, so all access should be done through this method
    KnnVectorsReader knnVectorsReader = ((CodecReader) reader).getVectorReader();
    KnnVectorsReader forField =
        ((PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader).getFieldReader(luceneFieldName);
    return ((Lucene99HnswVectorsReader) forField).getQuantizedVectorValues(luceneFieldName);
  }
}
