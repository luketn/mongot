package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

public record HasRootOperator(Score score, Operator operator) implements Operator {

  static class Fields {
    static final Field.Required<Operator> OPERATOR =
        Field.builder("operator")
            .classField(Operator::exactlyOneFromBson)
            .disallowUnknownFields()
            .required();
    public static final Field.WithDefault<Score> SCORE =
        Field.builder("score")
            .classField(Score::fromBsonAllowEmbedded)
            .disallowUnknownFields()
            .optional()
            .withDefault(Score.defaultScore());
  }

  @Override
  public Type getType() {
    return Type.HAS_ROOT;
  }

  static HasRootOperator fromBson(DocumentParser parser) throws BsonParseException {
    Operator operator = parser.getField(Fields.OPERATOR).unwrap();
    // No path validation is needed for HasRootOperator since it always invoke its operator
    // at the root level.
    return new HasRootOperator(parser.getField(Fields.SCORE).unwrap(), operator);
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score()).field(Fields.OPERATOR, this.operator).build();
  }
}
