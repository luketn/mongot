package com.xgen.mongot.index.query.scores.expressions;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.bson.BsonString;
import org.bson.BsonValue;

public record ScoreExpression() implements Expression {

  private static class Values {
    private static final Value.Required<String> SCORE =
        Value.builder()
            .stringValue()
            .validate(
                score ->
                    !score.equals("relevance")
                        ? Optional.of("score field must have value \"relevance\"")
                        : Optional.empty())
            .required();
  }

  /** Constructs a ScoreExpression from the supplied BsonValue. */
  static ScoreExpression fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    Values.SCORE.getParser().parse(context, bsonValue);
    return new ScoreExpression();
  }

  @Override
  public List<FieldPath> getPaths() {
    return List.of();
  }

  @Override
  public BsonValue expressionToBson() {
    return new BsonString("relevance");
  }
}
