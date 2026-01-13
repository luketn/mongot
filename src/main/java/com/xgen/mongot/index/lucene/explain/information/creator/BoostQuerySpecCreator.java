package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.BoostQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import org.apache.lucene.search.BoostQuery;

public class BoostQuerySpecCreator {
  static BoostQuerySpec fromQuery(
      BoostQuery query, QueryExecutionContextNode child, Explain.Verbosity verbosity) {
    return new BoostQuerySpec(
        QueryExplainInformationCreator.fromNode(child, verbosity), query.getBoost());
  }
}
