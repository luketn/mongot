package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.Operator;
import java.io.IOException;
import org.apache.lucene.search.Query;

/** Functional interface to create the Lucene query from Operator */
@FunctionalInterface
public interface QueryCreator {
  Query create(Operator operator, SingleQueryContext indexReader)
      throws InvalidQueryException, IOException;
}
