package com.xgen.mongot.index.query.scores.expressions;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import org.bson.BsonValue;

public record LogExpression(Expression argument) implements Expression {

  public static LogExpression fromBson(DocumentParser parser) throws BsonParseException {
    return new LogExpression(Expression.fromBson(parser));
  }

  @Override
  public List<FieldPath> getPaths() {
    return this.argument.getPaths();
  }

  @Override
  public BsonValue expressionToBson() {
    return this.argument.toBson();
  }
}
