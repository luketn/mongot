package com.xgen.mongot.index.query.scores.expressions;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonValue;

public record MultiplyExpression(List<Expression> arguments) implements Expression {

  private static class Values {
    private static final Value.Required<List<Expression>> ARGUMENTS =
        Value.builder()
            .listOf(
                Value.builder().classValue(Expression::fromBson).disallowUnknownFields().required())
            .validate(
                args ->
                    args.size() < 2
                        ? Optional.of("multiply expression must have at least two arguments")
                        : Optional.empty())
            .required();
  }

  public static MultiplyExpression fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    List<Expression> arguments = Values.ARGUMENTS.getParser().parse(context, bsonValue);
    return new MultiplyExpression(arguments);
  }

  @Override
  public List<FieldPath> getPaths() {
    return this.arguments.stream()
        .map(Expression::getPaths)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  public BsonValue expressionToBson() {
    return Values.ARGUMENTS.getEncoder().encode(this.arguments);
  }
}
