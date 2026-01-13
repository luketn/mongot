package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.MultiTermQueryConstantScoreBlendedWrapperSpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiTermQueryConstantScoreBlendedWrapperSpecCreator {
  static MultiTermQueryConstantScoreBlendedWrapperSpec fromQuery(
      Optional<? extends QueryChildren<? extends QueryExecutionContextNode>> children,
      Explain.Verbosity verbosity) {
    return new MultiTermQueryConstantScoreBlendedWrapperSpec(
        Optional.empty(), explainInfosFor(children, verbosity));
  }

  private static List<QueryExplainInformation> explainInfosFor(
      Optional<? extends QueryChildren<? extends QueryExecutionContextNode>> queryChildren,
      Explain.Verbosity verbosity) {
    if (queryChildren.isEmpty()) {
      return Collections.emptyList();
    }
    QueryChildren<? extends QueryExecutionContextNode> children = queryChildren.get();
    return Stream.of(children.must(), children.mustNot(), children.should(), children.filter())
        .flatMap(Collection::stream)
        .map(contextNode -> QueryExplainInformationCreator.fromNode(contextNode, verbosity))
        .collect(Collectors.toList());
  }
}
