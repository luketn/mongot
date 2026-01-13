package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.stream.Stream;
import org.bson.BsonValue;

public record SpanContainsOperator(
    Score score, SpanOperator little, SpanOperator big, SpanToReturn spanToReturn)
    implements SpanOperator {

  public static class Fields {
    static final Field.Required<SpanOperator> BIG =
        Field.builder("big")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<SpanOperator> LITTLE =
        Field.builder("little")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<SpanToReturn> SPAN_TO_RETURN =
        Field.builder("spanToReturn").enumField(SpanToReturn.class).asCamelCase().required();
  }

  public enum SpanToReturn {
    INNER,
    OUTER
  }

  public static SpanContainsOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new SpanContainsOperator(
        Operators.parseScore(parser),
        parser.getField(Fields.LITTLE).unwrap(),
        parser.getField(Fields.BIG).unwrap(),
        parser.getField(Fields.SPAN_TO_RETURN).unwrap());
  }

  @Override
  public BsonValue spanOperatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.LITTLE, this.little)
        .field(Fields.BIG, this.big)
        .field(Fields.SPAN_TO_RETURN, this.spanToReturn)
        .build();
  }

  @Override
  public List<StringPath> getPaths() {
    return Stream.concat(this.big.getPaths().stream(), this.little.getPaths().stream()).toList();
  }

  @Override
  public Type getType() {
    return Type.SPAN_CONTAINS;
  }
}
