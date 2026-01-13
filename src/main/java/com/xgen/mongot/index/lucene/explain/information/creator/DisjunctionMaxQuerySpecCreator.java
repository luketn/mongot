package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.DisjunctionMaxQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DisjunctionMaxQuerySpecCreator {
  static <T extends Optional<? extends QueryChildren<? extends QueryExecutionContextNode>>>
      DisjunctionMaxQuerySpec fromQuery(
          org.apache.lucene.search.DisjunctionMaxQuery query,
          T children,
          Explain.Verbosity verbosity) {
    return new DisjunctionMaxQuerySpec(
        forChildNodes(children, verbosity), query.getTieBreakerMultiplier());
  }

  private static <T extends Optional<? extends QueryChildren<? extends QueryExecutionContextNode>>>
      List<QueryExplainInformation> forChildNodes(T children, Explain.Verbosity verbosity) {
    return children
        // Dismax should only contain SHOULD children
        .map(QueryChildren::should)
        .map(
            contextNodes ->
                contextNodes.stream()
                    .map(
                        contextNode ->
                            QueryExplainInformationCreator.fromNode(contextNode, verbosity))
                    .collect(Collectors.toList()))
        .orElse(new ArrayList<>());
  }
}
