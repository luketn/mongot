package com.xgen.mongot.index.lucene.query.weights;

import java.util.Optional;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.PhraseWeight;
import org.apache.lucene.search.similarities.Similarity;

/**
 * {@link SafePhraseWeight} wraps {@link PhraseWeight} to modify the description Lucene generates in
 * its {@link Explanation} when running score details over phrase queries.
 *
 * <p>The existing {@link PhraseWeight#explain(LeafReaderContext, int)} method incorporates the
 * internal Lucene document id in its output, which is not equivalent to the _id of the MongoDB
 * document. As a result, in {@link SafePhraseWeight#explain(LeafReaderContext, int)}, we remove any
 * mention of document ids at all.
 */
class SafePhraseWeight extends WrappedExplainWeight {
  final Similarity similarity;

  SafePhraseWeight(PhraseWeight phraseWeight, Similarity similarity) {
    super(phraseWeight);
    this.similarity = similarity;
  }

  @Override
  Optional<String> matchRewrite() {
    return Optional.of(
        String.format(
            "%s [%s], result of:", this.parentQuery, this.similarity.getClass().getSimpleName()));
  }
}
