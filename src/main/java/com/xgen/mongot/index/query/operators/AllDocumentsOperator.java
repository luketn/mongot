package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * An operator that matches all top-level documents. It roughly corresponds to Lucene's
 * MatchAllDocsQuery.
 */
public record AllDocumentsOperator(Score score) implements Operator {
  public static final AllDocumentsOperator INSTANCE =
      new AllDocumentsOperator(Score.defaultScore());

  @Override
  public Type getType() {
    return Type.ALL_DOCUMENTS;
  }

  /**
   * This operator doesn't have an explicit representation in BSON. It's used as a fallback in the
   * absence of other operators.
   */
  @Override
  public BsonValue operatorToBson() {
    return new BsonDocument();
  }
}
