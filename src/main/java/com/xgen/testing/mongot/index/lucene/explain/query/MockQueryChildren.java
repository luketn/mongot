package com.xgen.testing.mongot.index.lucene.explain.query;

import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.BooleanClause;

public record MockQueryChildren(
    List<QueryExecutionContextNode> must,
    List<QueryExecutionContextNode> mustNot,
    List<QueryExecutionContextNode> should,
    List<QueryExecutionContextNode> filter)
    implements QueryChildren<QueryExecutionContextNode> {

  @Override
  public void addClause(QueryExecutionContextNode child, BooleanClause.Occur occur) {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public Optional<BooleanClause.Occur> occurFor(QueryExecutionContextNode child) {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public void removeChild(QueryExecutionContextNode child) {
    throw new NotImplementedException("not implemented");
  }
}
