package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.IndexOrDocValuesQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.util.Optional;
import java.util.stream.Collectors;

public class IndexOrDocValuesQuerySpecCreator {
  static <T extends Optional<? extends QueryChildren<? extends QueryExecutionContextNode>>>
      IndexOrDocValuesQuerySpec fromQuery(
          org.apache.lucene.search.IndexOrDocValuesQuery query,
          T children,
          Explain.Verbosity verbosity) {
    return new IndexOrDocValuesQuerySpec(
        children
            .map(QueryChildren::must)
            .map(
                clauses ->
                    clauses.stream()
                        .map(node -> QueryExplainInformationCreator.fromNode(node, verbosity))
                        .collect(Collectors.toList())));
  }
}
