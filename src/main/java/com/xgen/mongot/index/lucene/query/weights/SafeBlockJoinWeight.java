package com.xgen.mongot.index.lucene.query.weights;

import java.util.Optional;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;

/**
 * {@link SafeBlockJoinWeight} wraps the Weight created by {@link ToParentBlockJoinQuery} in order
 * to modify the description Lucene generates in its {@link Explanation} when running score details
 * over embeddedDocument queries.
 *
 * <p>The existing {@link ToParentBlockJoinQuery.BlockJoinWeight#explain(LeafReaderContext, int)}
 * method incorporates the internal Lucene document id in its output, which is not equivalent to the
 * _id of the MongoDB document. As a result, in {@link
 * SafeBlockJoinWeight#explain(LeafReaderContext, int)}, we remove any mention of document ids at
 * all.
 */
class SafeBlockJoinWeight extends WrappedExplainWeight {
  SafeBlockJoinWeight(Weight weight) {
    super(weight);
  }

  @Override
  Optional<String> matchRewrite() {
    return Optional.of("Score based on child docs, best match:");
  }
}
