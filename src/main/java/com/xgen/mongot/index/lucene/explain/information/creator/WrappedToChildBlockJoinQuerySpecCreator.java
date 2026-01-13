package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.WrappedToChildBlockJoinQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;

public class WrappedToChildBlockJoinQuerySpecCreator {
  static WrappedToChildBlockJoinQuerySpec fromQuery(
      QueryExecutionContextNode child, Explain.Verbosity verbosity) {
    return new WrappedToChildBlockJoinQuerySpec(
        QueryExplainInformationCreator.fromNode(child, verbosity));
  }
}
