package com.xgen.mongot.index;

public interface InitializedVectorIndex extends InitializedIndex, VectorIndex {
  /**
   * Returns the IndexReader for the Index.
   *
   * @throws IllegalStateException if the Index is closed.
   */
  @Override
  VectorIndexReader getReader();
}
