package com.xgen.mongot.index.lucene.explain.information.creator;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.WrappedKnnQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.util.Check;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class WrappedKnnQuerySpecCreator {
  static <T extends Optional<? extends QueryChildren<?>>> WrappedKnnQuerySpec fromQuery(
      T children, Explain.Verbosity verbosity) {
    var parsedQueryList =
        children.map(QueryChildren::must).map(clauses -> forChildren(clauses, verbosity));
    var parsedQuery = Check.isPresent(parsedQueryList, "query");

    var parsedFilterList =
        children.map(QueryChildren::filter).map(clauses -> forChildren(clauses, verbosity));

    var parsedFilter = Check.isPresent(parsedFilterList, "filter");
    checkArg(parsedFilter.size() <= 1, "Can only have at most one top-level filter");

    return new WrappedKnnQuerySpec(
        parsedQuery, parsedFilter.isEmpty() ? Optional.empty() : Optional.of(parsedFilter.get(0)));
  }

  static List<QueryExplainInformation> forChildren(
      Collection<? extends QueryExecutionContextNode> nodes, Explain.Verbosity verbosity) {
    return nodes.stream()
        .map(contextNode -> QueryExplainInformationCreator.fromNode(contextNode, verbosity))
        .collect(Collectors.toList());
  }
}
