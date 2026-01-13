package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.ExactVectorSearchQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery;
import java.util.Optional;

public class ExactVectorSearchQuerySpecCreator {
  static ExactVectorSearchQuerySpec fromQuery(
      ExactVectorSearchQuery q, QueryExecutionContextNode child, Explain.Verbosity verbosity) {
    return new ExactVectorSearchQuerySpec(
        q.getField(),
        q.getSimilarityFunction(),
        Optional.of(QueryExplainInformationCreator.fromNode(child, verbosity))
            .filter(filter -> filter.type() != LuceneQuerySpecification.Type.MATCH_ALL_DOCS_QUERY));
  }
}
