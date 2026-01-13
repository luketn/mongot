package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.WrappedToParentBlockJoinQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;

public class WrappedToParentBlockJoinQuerySpecCreator {
  static WrappedToParentBlockJoinQuerySpec fromQuery(
      QueryExecutionContextNode child, Explain.Verbosity verbosity) {
    return new WrappedToParentBlockJoinQuerySpec(
        QueryExplainInformationCreator.fromNode(child, verbosity));
  }
}
