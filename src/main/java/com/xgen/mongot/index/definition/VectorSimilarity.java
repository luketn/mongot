package com.xgen.mongot.index.definition;

import org.apache.lucene.index.VectorSimilarityFunction;

public enum VectorSimilarity {
  EUCLIDEAN(VectorSimilarityFunction.EUCLIDEAN),
  DOT_PRODUCT(VectorSimilarityFunction.DOT_PRODUCT),
  COSINE(VectorSimilarityFunction.COSINE);

  private final VectorSimilarityFunction luceneSimilarityFunction;

  VectorSimilarity(VectorSimilarityFunction luceneSimilarityFunction) {
    this.luceneSimilarityFunction = luceneSimilarityFunction;
  }

  public VectorSimilarityFunction getLuceneSimilarityFunction() {
    return this.luceneSimilarityFunction;
  }
}
