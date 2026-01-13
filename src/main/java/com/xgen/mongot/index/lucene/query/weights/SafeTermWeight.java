package com.xgen.mongot.index.lucene.query.weights;

import java.util.Optional;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;

/**
 * {@link SafeTermWeight} wraps {@link TermQuery.TermWeight} to modify the description Lucene
 * generates in its {@link Explanation} when running score details over {@link TermQuery.TermWeight}
 * queries.
 *
 * <p>The existing {@link TermQuery.TermWeight#explain(LeafReaderContext, int)} method incorporates
 * the internal Lucene document id in its output, which is not equivalent to the _id of the MongoDB
 * document. As a result, in the description of {@link SafeTermWeight#explain(LeafReaderContext,
 * int)}, we remove any mention of document ids at all.
 */
class SafeTermWeight extends WrappedExplainWeight {
  final Similarity similarity;

  SafeTermWeight(Weight termWeight, Similarity similarity) {
    super(termWeight);
    this.similarity = similarity;
  }

  @Override
  Optional<String> matchRewrite() {
    String similarityName;
    if (this.similarity instanceof PerFieldSimilarityWrapper wrapper
        && this.parentQuery instanceof TermQuery termQuery) {
      String field = termQuery.getTerm().field();
      Similarity fieldSimilarity = wrapper.get(field);
      similarityName = fieldSimilarity.getClass().getSimpleName();
    } else {
      similarityName = this.similarity.getClass().getSimpleName();
    }
    return Optional.of(String.format("%s [%s], result of:", this.parentQuery, similarityName));
  }
}
