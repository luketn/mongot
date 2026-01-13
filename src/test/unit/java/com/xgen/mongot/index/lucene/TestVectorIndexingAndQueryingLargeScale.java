package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.VectorIndexingAndQueryingTestHarness.buildVectorIndexDefinition;

import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.bson.Vector;
import java.util.Arrays;
import java.util.List;
import org.bson.BsonDocument;

// These tests with 10k vectors can take significant time to run and can also timeout.
// To avoid this, the tests are split into three different subclasses of this class so that
// each test class can be executed independently by bazel.
public class TestVectorIndexingAndQueryingLargeScale {

  static final int NUM_VECTORS = 10000;
  static final int NUM_QUERIES = 100;
  static final int NUM_CANDIDATES = 500;
  static final int LIMIT = 50;
  static final int DIMENSIONS = 1024;

  final VectorSimilarity vectorSimilarity;
  final boolean exact;
  final int numPartitions;
  final VectorIndexingAndQueryingTestHarness testHarness;
  List<BsonDocument> docs;
  List<? extends Vector> queryVectors;

  public TestVectorIndexingAndQueryingLargeScale(
      VectorSimilarity vectorSimilarity, boolean exact, int numPartitions) {
    this.vectorSimilarity = vectorSimilarity;
    this.exact = exact;
    this.numPartitions = numPartitions;
    this.testHarness = new VectorIndexingAndQueryingTestHarness();
  }

  static Object[][] getTestParams() {
    return new Object[][] {
      {VectorSimilarity.DOT_PRODUCT, false, 1},
      {VectorSimilarity.DOT_PRODUCT, true, 1},
      {VectorSimilarity.DOT_PRODUCT, false, 2},
      {VectorSimilarity.DOT_PRODUCT, true, 2},
      {VectorSimilarity.COSINE, false, 1},
      {VectorSimilarity.COSINE, true, 1},
      {VectorSimilarity.COSINE, false, 2},
      {VectorSimilarity.COSINE, true, 2},
      {VectorSimilarity.EUCLIDEAN, false, 1},
      {VectorSimilarity.EUCLIDEAN, true, 1},
      {VectorSimilarity.EUCLIDEAN, false, 2},
      {VectorSimilarity.EUCLIDEAN, true, 2},
    };
  }

  void runTest(Vector.VectorType vectorType) throws Exception {
    runTest(vectorType, VectorQuantization.NONE);
  }

  void runTest(Vector.VectorType vectorType, VectorQuantization vectorQuantization)
      throws Exception {
    try (var harness = this.testHarness) {
      harness.setNumCandidatesAndLimit(NUM_CANDIDATES, LIMIT);
      harness.setUp(
          buildVectorIndexDefinition(
              this.vectorSimilarity, DIMENSIONS, this.numPartitions, vectorQuantization));
      switch (vectorType) {
        case FLOAT -> {
          float[][] floatVectors =
              harness.generateNormalizedRandomFloatVectors(NUM_VECTORS, DIMENSIONS);
          this.docs =
              harness.createVectorDocs(
                  floatVectors, 0, VectorIndexingAndQueryingTestHarness::fromNativeFloats);
          this.queryVectors =
              Arrays.stream(harness.generateNormalizedRandomFloatVectors(NUM_QUERIES, DIMENSIONS))
                  .map(VectorIndexingAndQueryingTestHarness::fromNativeFloats)
                  .toList();
        }
        case BYTE -> {
          byte[][] byteVectors = harness.generateRandomByteVectors(NUM_VECTORS, DIMENSIONS);
          this.docs = harness.createVectorDocs(byteVectors, 0, Vector::fromBytes);
          this.queryVectors =
              Arrays.stream(harness.generateRandomByteVectors(NUM_QUERIES, DIMENSIONS))
                  .map(Vector::fromBytes)
                  .toList();
        }
        case BIT -> {
          int vectorLength = DIMENSIONS / Byte.SIZE;
          byte[][] byteVectors = harness.generateRandomByteVectors(NUM_VECTORS, vectorLength);
          this.docs = harness.createVectorDocs(byteVectors, 0, Vector::fromBits);
          this.queryVectors =
              Arrays.stream(harness.generateRandomByteVectors(NUM_QUERIES, vectorLength))
                  .map(Vector::fromBits)
                  .toList();
        }
      }
      harness.runTestWithRecallCheck(
          this.docs, this.queryVectors, this.exact, this.vectorSimilarity);
    }
  }
}
