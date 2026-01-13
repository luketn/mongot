package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import org.bson.BsonValue;

public record FunctionScore(Expression expression) implements Score {

  public static FunctionScore fromBson(DocumentParser parser) throws BsonParseException {
    return new FunctionScore(Expression.fromBson(parser));
  }

  @Override
  public BsonValue scoreToBson() {
    return this.expression.toBson();
  }

  @Override
  public Type getType() {
    return Type.FUNCTION;
  }

  @Override
  public List<FieldPath> getChildPaths() {
    return this.expression.getPaths();
  }
}
