package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.stream.Stream;
import org.bson.BsonValue;

public record SpanSubtractOperator(Score score, SpanOperator include, SpanOperator exclude)
    implements SpanOperator {

  public static class Fields {
    public static final Field.Required<SpanOperator> INCLUDE =
        Field.builder("include")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Required<SpanOperator> EXCLUDE =
        Field.builder("exclude")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .required();
  }

  public static SpanSubtractOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new SpanSubtractOperator(
        Operators.parseScore(parser),
        parser.getField(Fields.INCLUDE).unwrap(),
        parser.getField(Fields.EXCLUDE).unwrap());
  }

  @Override
  public BsonValue spanOperatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.INCLUDE, this.include)
        .field(Fields.EXCLUDE, this.exclude)
        .build();
  }

  @Override
  public List<StringPath> getPaths() {
    return Stream.concat(this.include.getPaths().stream(), this.exclude.getPaths().stream())
        .toList();
  }

  @Override
  public Type getType() {
    return Type.SPAN_SUBTRACT;
  }
}
