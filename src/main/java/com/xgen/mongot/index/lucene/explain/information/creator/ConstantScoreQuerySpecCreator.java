package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.ConstantScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;

public class ConstantScoreQuerySpecCreator {
  static ConstantScoreQuerySpec fromQuery(
      org.apache.lucene.search.ConstantScoreQuery query,
      QueryExecutionContextNode child,
      Explain.Verbosity verbosity) {
    return new ConstantScoreQuerySpec(QueryExplainInformationCreator.fromNode(child, verbosity));
  }
}
