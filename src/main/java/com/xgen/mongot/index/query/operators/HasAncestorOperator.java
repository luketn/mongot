package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

public record HasAncestorOperator(Score score, FieldPath ancestorPath, Operator operator)
    implements Operator {

  static class Fields {
    static final Field.Required<String> ANCESTOR_PATH =
        Field.builder("ancestorPath").stringField().required();
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
    return Type.HAS_ANCESTOR;
  }

  static HasAncestorOperator fromBson(DocumentParser parser) throws BsonParseException {
    FieldPath ancestorPath = FieldPath.parse(parser.getField(Fields.ANCESTOR_PATH).unwrap());
    Operator operator = parser.getField(Fields.OPERATOR).unwrap();
    return new HasAncestorOperator(parser.getField(Fields.SCORE).unwrap(), ancestorPath, operator);
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.ANCESTOR_PATH, this.ancestorPath.toString())
        .field(Fields.OPERATOR, this.operator)
        .build();
  }
}
