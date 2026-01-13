package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

public record SpanNearOperator(Score score, List<SpanOperator> clauses, int slop, boolean inOrder)
    implements SpanOperator {

  public static class Fields {
    public static final Field.Required<List<SpanOperator>> CLAUSES =
        Field.builder("clauses")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .required();

    public static final Field.WithDefault<Boolean> IN_ORDER =
        Field.builder("inOrder").booleanField().optional().withDefault(false);

    public static final Field.WithDefault<Integer> SLOP =
        Field.builder("slop").intField().mustBeNonNegative().optional().withDefault(0);
  }

  public static SpanNearOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new SpanNearOperator(
        Operators.parseScore(parser),
        parser.getField(Fields.CLAUSES).unwrap(),
        parser.getField(Fields.SLOP).unwrap(),
        parser.getField(Fields.IN_ORDER).unwrap());
  }

  @Override
  public BsonValue spanOperatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.CLAUSES, this.clauses)
        .field(Fields.IN_ORDER, this.inOrder)
        .field(Fields.SLOP, this.slop)
        .build();
  }

  @Override
  public List<StringPath> getPaths() {
    return this.clauses.stream().map(SpanOperator::getPaths).flatMap(List::stream).toList();
  }

  @Override
  public Type getType() {
    return Type.SPAN_NEAR;
  }
}
