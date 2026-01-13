package com.xgen.mongot.index.query.scores.expressions;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public record ConstantExpression(double constant) implements Expression {

  private static class Values {
    private static final Value.Required<Double> CONSTANT =
        Value.builder().doubleValue().mustBeFinite().required();
  }

  /** Constructs a ConstantExpression from the supplied BsonValue. */
  static ConstantExpression fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    double constant = Values.CONSTANT.getParser().parse(context, bsonValue);
    return new ConstantExpression(constant);
  }

  @Override
  public List<FieldPath> getPaths() {
    return List.of();
  }

  @Override
  public BsonValue expressionToBson() {
    return new BsonDouble(this.constant);
  }
}
