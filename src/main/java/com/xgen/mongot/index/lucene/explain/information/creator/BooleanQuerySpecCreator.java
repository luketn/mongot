package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.BooleanQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BooleanQuerySpecCreator {
  static <T extends Optional<? extends QueryChildren<? extends QueryExecutionContextNode>>>
      BooleanQuerySpec fromQuery(
          org.apache.lucene.search.BooleanQuery query, T children, Explain.Verbosity verbosity) {
    return new BooleanQuerySpec(
        children.map(QueryChildren::must).map(clauses -> forChildNodes(clauses, verbosity)),
        children.map(QueryChildren::mustNot).map(clauses -> forChildNodes(clauses, verbosity)),
        children.map(QueryChildren::should).map(clauses -> forChildNodes(clauses, verbosity)),
        children.map(QueryChildren::filter).map(clauses -> forChildNodes(clauses, verbosity)),
        Optional.of(query.getMinimumNumberShouldMatch()));
  }

  private static List<QueryExplainInformation> forChildNodes(
      Collection<? extends QueryExecutionContextNode> nodes, Explain.Verbosity verbosity) {
    return nodes.stream()
        .map(contextNode -> QueryExplainInformationCreator.fromNode(contextNode, verbosity))
        .collect(Collectors.toList());
  }
}
