package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.FunctionScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;

public class FunctionScoreQuerySpecCreator {
  static FunctionScoreQuerySpec fromQuery(
      org.apache.lucene.queries.function.FunctionScoreQuery query,
      QueryExecutionContextNode child,
      Explain.Verbosity verbosity) {
    return new FunctionScoreQuerySpec(
        query.getSource().toString(), QueryExplainInformationCreator.fromNode(child, verbosity));
  }
}
