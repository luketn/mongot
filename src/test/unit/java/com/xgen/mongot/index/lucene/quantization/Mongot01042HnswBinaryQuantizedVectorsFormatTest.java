package com.xgen.mongot.index.lucene.quantization;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;

public class Mongot01042HnswBinaryQuantizedVectorsFormatTest extends BaseKnnVectorsFormatTestCase {
  private final KnnVectorsFormat format = new Mongot01042HnswBinaryQuantizedVectorsFormat();

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return Mongot01042HnswBinaryQuantizedVectorsFormatTest.this.format;
      }
    };
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    return VectorEncoding.BYTE;
  }

  @Override
  protected VectorSimilarityFunction randomSimilarity() {
    return VectorSimilarityFunction.EUCLIDEAN;
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase randomVector().
  public static float[] randomPositiveNegativeVector(int dim) {
    assert dim > 0;
    float[] v = new float[dim];
    @Var double squareSum = 0.0;
    // keep generating until we don't get a zero-length vector
    while (squareSum == 0.0) {
      squareSum = 0.0;
      for (int i = 0; i < dim; i++) {
        v[i] = (random().nextFloat() * 2.0f) - 1.0f;
        //        v[i] = random().nextFloat();
        squareSum += v[i] * v[i];
      }
    }
    return v;
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase randomNormalizedVector().
  public static float[] randomPositiveNegativeNormalizedVector(int dim) {
    float[] v = randomPositiveNegativeVector(dim);
    VectorUtil.l2normalize(v);
    return v;
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase testRandomWithUpdatesAndGraph().
  @Override
  public void testRandomWithUpdatesAndGraph() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    String fieldName = "field";
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      @Var int dimension = atLeast(10);
      if (dimension % 2 != 0) {
        dimension++;
      }
      float[][] id2value = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        int id = random().nextInt(numDoc);
        float[] value;
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          value = randomPositiveNegativeNormalizedVector(dimension);
        } else {
          value = null;
        }
        id2value[id] = value;
        add2(iw, fieldName, id, value, VectorSimilarityFunction.EUCLIDEAN);
      }
      try (IndexReader reader = DirectoryReader.open(iw)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          Bits liveDocs = ctx.reader().getLiveDocs();
          FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          StoredFields storedFields = ctx.reader().storedFields();
          @Var int docId;
          @Var int numLiveDocsWithVectors = 0;
          while ((docId = vectorValues.nextDoc()) != NO_MORE_DOCS) {
            float[] v = vectorValues.vectorValue();
            assertEquals(dimension, v.length);
            String idString = storedFields.document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            if (liveDocs == null || liveDocs.get(docId)) {
              assertArrayEquals(
                  "values differ for id=" + idString + ", docid=" + docId + " leaf=" + ctx.ord,
                  id2value[id],
                  v,
                  0);
              numLiveDocsWithVectors++;
            } else {
              if (id2value[id] != null) {
                assertFalse(Arrays.equals(id2value[id], v));
              }
            }
          }

          if (numLiveDocsWithVectors == 0) {
            continue;
          }

          // assert that searchNearestVectors returns the expected number of documents,
          // in descending score order
          int size = ctx.reader().getFloatVectorValues(fieldName).size();
          @Var int k = random().nextInt(size / 2 + 1) + 1;
          if (k > numLiveDocsWithVectors) {
            k = numLiveDocsWithVectors;
          }
          TopDocs results =
              ctx.reader()
                  .searchNearestVectors(
                      fieldName, randomNormalizedVector(dimension), k, liveDocs, Integer.MAX_VALUE);
          assertEquals(Math.min(k, size), results.scoreDocs.length);
          for (int i = 0; i < k - 1; i++) {
            assertTrue(results.scoreDocs[i].score >= results.scoreDocs[i + 1].score);
          }
        }
      }
    }
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase add().
  private void add2(
      IndexWriter iw,
      String field,
      int id,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    add2(iw, field, id, random().nextInt(100), vector, similarityFunction);
  }

  // Copied from Lucene's BaseKnnVectorsFormatTestCase add().
  private void add2(
      IndexWriter iw,
      String field,
      int id,
      int sortkey,
      float[] vector,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new KnnFloatVectorField(field, vector, similarityFunction));
    }
    doc.add(new NumericDocValuesField("sortkey", sortkey));
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    Term idTerm = new Term("id", idString);
    iw.updateDocument(idTerm, doc);
  }
}
