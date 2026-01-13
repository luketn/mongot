package com.xgen.mongot.index.lucene.query.util;

import java.util.Map;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;

public class Rescoring {

  /**
   * Change the score of documents returned in a query by function and variables.
   *
   * @param original original query to modify scores of.
   * @param scoringFunction expression to evaluate to get new score.
   * @param variableMappings string variable names mapped to value sources.
   * @return A re-scored query.
   */
  public static Query rewriteScore(
      Query original,
      Expression scoringFunction,
      Map<String, DoubleValuesSource> variableMappings) {
    SimpleBindings bindings = new SimpleBindings();
    variableMappings.forEach(bindings::add);
    return new FunctionScoreQuery(original, scoringFunction.getDoubleValuesSource(bindings));
  }
}
