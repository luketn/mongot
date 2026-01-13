package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

/**
 * SpanFirstOperatorDefinition.
 *
 * @param operator child span.
 * @param endingPosition lastPosition to consider in span
 */
public record SpanFirstOperator(Score score, SpanOperator operator, int endingPosition)
    implements SpanOperator {

  public static class Fields {
    public static final Field.Required<SpanOperator> OPERATOR =
        Field.builder("operator")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .required();

    public static final Field.WithDefault<Integer> END_POSITION_LTE =
        Field.builder("endPositionLte").intField().mustBePositive().optional().withDefault(3);
  }

  public static SpanFirstOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new SpanFirstOperator(
        Operators.parseScore(parser),
        parser.getField(Fields.OPERATOR).unwrap(),
        parser.getField(Fields.END_POSITION_LTE).unwrap());
  }

  @Override
  public BsonValue spanOperatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.OPERATOR, this.operator)
        .field(Fields.END_POSITION_LTE, this.endingPosition)
        .build();
  }

  @Override
  public List<StringPath> getPaths() {
    return this.operator.getPaths();
  }

  @Override
  public Type getType() {
    return Type.SPAN_FIRST;
  }
}
