package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.NUM_PARTITIONS;

import com.xgen.mongot.index.MeteredVectorIndexReader;
import com.xgen.mongot.util.bson.Vector;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMultiLuceneVectorIndexReader {

  private VectorIndexingAndQueryingTestHarness testHarness;

  /** Set up resources for test. */
  @Before
  public void setUp() throws Exception {
    this.testHarness = new VectorIndexingAndQueryingTestHarness();
    this.testHarness.setUp(MOCK_VECTOR_MULTI_INDEX_PARTITION_DEFINITION);

    var indexReader = this.testHarness.getReader();
    Assert.assertTrue(
        ((MeteredVectorIndexReader) indexReader).unwrap()
            instanceof MultiLuceneVectorIndexReader);
    var multiReader =
        (MultiLuceneVectorIndexReader) ((MeteredVectorIndexReader) indexReader).unwrap();
    Assert.assertEquals(NUM_PARTITIONS, multiReader.numUnderlyingReaders());
  }

  @After
  public void tearDown() throws Exception {
    this.testHarness.close();
  }

  @Test
  public void testNoDocumentWithFloatVector()
      throws Exception {
    Vector queryVector = Vector.fromFloats(new float[] {1f, 0f, 0f}, NATIVE);
    this.testHarness.runTest(Collections.emptyList(), queryVector, Collections.emptyList());
  }

  @Test
  public void testSingleDocumentWithFloatVector() throws Exception {
    float[][] singleVector = new float[][] {{1f, 0f, 0f}};
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 99, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    List<Integer> expectedDocIds = List.of(99);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testMultipleDocumentsWithFloatVectorInAscendingRadians() throws Exception {
    // Unit vectors with ascending radians. Since the query vector lies on the x-axis, the
    // Cosine similarity function will score the vectors in descending order.
    float[][] singleVector = new float[][] {
        {1.0000f, 0.0000f, 0f},
        {0.9969f, 0.0785f, 0f},
        {0.9877f, 0.1564f, 0f},
        {0.9724f, 0.2334f, 0f},
        {0.9511f, 0.3090f, 0f},
        {0.9239f, 0.3827f, 0f},
        {0.8910f, 0.4540f, 0f},
        {0.8526f, 0.5225f, 0f},
        {0.8090f, 0.5878f, 0f},
        {0.7604f, 0.6494f, 0f},
        {0.7071f, 0.7071f, 0f},
        {0.6494f, 0.7604f, 0f},
        {0.5878f, 0.8090f, 0f},
        {0.5225f, 0.8526f, 0f},
        {0.4540f, 0.8910f, 0f},
        {0.3827f, 0.9239f, 0f}};
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    // The query has a limit of 10, so only the top 10 docs will be returned.
    List<Integer> expectedDocIds = List.of(15, 14, 13, 12, 11, 10, 9, 8, 7, 6);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testMultipleDocumentsWithFloatVectorInDescendingRadians() throws Exception {
    // Unit vectors with descending radians. Since the query vector lies on the x-axis, the
    // Cosine similarity function will score the vectors in ascending order.
    float[][] singleVector = new float[][] {
        {0.0000f, 1.0000f, 0f},
        {0.0784f, 0.9969f, 0f},
        {0.1564f, 0.9876f, 0f},
        {0.2334f, 0.9723f, 0f},
        {0.3090f, 0.9510f, 0f},
        {0.3826f, 0.9238f, 0f},
        {0.4539f, 0.8910f, 0f},
        {0.5224f, 0.8526f, 0f},
        {0.5877f, 0.8090f, 0f},
        {0.6494f, 0.7604f, 0f},
        {0.7071f, 0.7071f, 0f},
        {0.7604f, 0.6494f, 0f},
        {0.8090f, 0.5877f, 0f},
        {0.8526f, 0.5224f, 0f},
        {0.8910f, 0.4539f, 0f},
        {0.9238f, 0.3826f, 0f}};
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0f, 1f, 0f}, NATIVE);
    // The query has a limit of 10, so only the top 10 docs will be returned.
    List<Integer> expectedDocIds = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }

  @Test
  public void testMultipleDocumentsWithRandomFloatVector() throws Exception {
    // Random unit vectors in the 3d space.
    float[][] singleVector = new float[][] {
        {0.0065f, 0.5351f, 0.8447f},
        {0.7103f, 0.0593f, 0.7013f},
        {0.6611f, 0.5433f, 0.5173f},
        {0.7586f, 0.6193f, 0.2022f},
        {0.0759f, 0.9922f, 0.0983f},
        {0.9583f, 0.2769f, 0.0694f},
        {0.1365f, 0.1073f, 0.9848f},
        {0.9239f, 0.3749f, 0.0751f},
        {0.6948f, 0.3089f, 0.6494f},
        {0.3616f, 0.4854f, 0.7959f},
        {0.6242f, 0.5859f, 0.5166f},
        {0.6708f, 0.6858f, 0.2821f},
        {0.8722f, 0.1227f, 0.4734f},
        {0.7160f, 0.0256f, 0.6975f},
        {0.1150f, 0.3623f, 0.9249f},
        {0.6639f, 0.5555f, 0.5005f},
        {0.6888f, 0.0375f, 0.7239f},
        {0.5447f, 0.5500f, 0.6329f},
        {0.7632f, 0.4875f, 0.4239f},
        {0.4668f, 0.2598f, 0.8452f}};
    var docs =
        this.testHarness.createVectorDocs(
            singleVector, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
    Vector queryVector = Vector.fromFloats(new float[] {0.9826f, 0.1790f, 0.0483f}, NATIVE);
    // The query has a limit of 10, so only the top 10 docs will be returned.
    List<Integer> expectedDocIds = List.of(5, 7, 12, 3, 18, 11, 15, 2, 8, 10);

    this.testHarness.runTest(docs, queryVector, expectedDocIds);
  }
}
