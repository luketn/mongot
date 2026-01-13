package com.xgen.mongot.index.definition;

/**
 * Defines the indexing algorithm used for vector search.
 *
 * <p>Has 2 implementations:
 *
 * @see HnswIndexingAlgorithm
 * @see FlatIndexingAlgorithm
 */
public sealed interface VectorIndexingAlgorithm {

  enum AlgorithmType {
    HNSW,
    FLAT
  }

  AlgorithmType type();

  record HnswIndexingAlgorithm(VectorFieldSpecification.HnswOptions options)
      implements VectorIndexingAlgorithm {

    public static final int MAXIMUM_MAX_EDGES = 64;
    public static final int DEFAULT_MAX_EDGES = 16;
    public static final int MAXIMUM_NUM_EDGE_CANDIDATES = 3200;
    public static final int DEFAULT_NUM_EDGE_CANDIDATES = 100;

    public static final VectorFieldSpecification.HnswOptions DEFAULT_HNSW_OPTIONS =
        new VectorFieldSpecification.HnswOptions(DEFAULT_MAX_EDGES, DEFAULT_NUM_EDGE_CANDIDATES);

    public HnswIndexingAlgorithm() {
      this(DEFAULT_HNSW_OPTIONS);
    }

    @Override
    public AlgorithmType type() {
      return AlgorithmType.HNSW;
    }
  }

  record FlatIndexingAlgorithm() implements VectorIndexingAlgorithm {
    @Override
    public AlgorithmType type() {
      return AlgorithmType.FLAT;
    }
  }
}
