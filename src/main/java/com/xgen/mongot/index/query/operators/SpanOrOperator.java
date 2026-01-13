package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

public record SpanOrOperator(Score score, List<SpanOperator> clauses) implements SpanOperator {

  public static class Fields {
    public static final Field.Required<List<SpanOperator>> CLAUSES =
        Field.builder("clauses")
            .classField(SpanOperator::fromBson, SpanOperator::operatorToBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .required();
  }

  public static SpanOrOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new SpanOrOperator(
        Operators.parseScore(parser), parser.getField(Fields.CLAUSES).unwrap());
  }

  @Override
  public BsonValue spanOperatorToBson() {
    return Operators.documentBuilder(score()).field(Fields.CLAUSES, this.clauses).build();
  }

  @Override
  public List<StringPath> getPaths() {
    return this.clauses.stream().map(SpanOperator::getPaths).flatMap(List::stream).toList();
  }

  @Override
  public Type getType() {
    return Type.SPAN_OR;
  }
}
