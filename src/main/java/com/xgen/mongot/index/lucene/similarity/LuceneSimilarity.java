package com.xgen.mongot.index.lucene.similarity;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;

public class LuceneSimilarity {
  static final Similarity DEFAULT_BM25_SIMILARITY = new BM25Similarity();

  /**
   * Creates a {@link Similarity} function that is potentially configured on a per-field basis.
   * Currently, all dynamically discovered fields will use the default BM25Similarity. As such, we
   * won't have to update the PerFieldSimilarityWrapper when a field is added.
   *
   * <p>Any fields that don't exist in it will simply use the proper default similarity. This may
   * change if we add dynamic templates, allowing for different dynamically mapped fields to have
   * non-default Similarities. If that happens this design may need to be reconsidered.
   */
  public static Similarity from(SearchIndexDefinition indexDefinition) {
    ImmutableMap<String, Similarity> mapping =
        new SimilarityWrapperBuilder().build(indexDefinition);
    if (mapping.isEmpty()) {
      return DEFAULT_BM25_SIMILARITY;
    }
    return new SimilarityWrapper(mapping, DEFAULT_BM25_SIMILARITY);
  }
}
