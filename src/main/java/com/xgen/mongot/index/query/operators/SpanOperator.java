package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public sealed interface SpanOperator extends Operator
    permits SpanContainsOperator,
        SpanFirstOperator,
        SpanNearOperator,
        SpanOrOperator,
        SpanSubtractOperator,
        SpanTermOperator {

  public static class Fields {
    public static final Field.Optional<SpanContainsOperator> CONTAINS =
        Fields.build("contains", SpanContainsOperator::fromBson);

    public static final Field.Optional<SpanFirstOperator> FIRST =
        Fields.build("first", SpanFirstOperator::fromBson);

    public static final Field.Optional<SpanNearOperator> NEAR =
        Fields.build("near", SpanNearOperator::fromBson);

    public static final Field.Optional<SpanOrOperator> OR =
        Fields.build("or", SpanOrOperator::fromBson);

    public static final Field.Optional<SpanSubtractOperator> SUBTRACT =
        Fields.build("subtract", SpanSubtractOperator::fromBson);

    public static final Field.Optional<SpanTermOperator> TERM =
        Fields.build("term", SpanTermOperator::fromBson);

    private static <T extends SpanOperator> Field.Optional<T> build(
        String name, ClassField.FromDocumentParser<T> parser) {
      return Field.builder(name)
          .classField(parser, SpanOperator::spanOperatorToBson)
          .disallowUnknownFields()
          .optional()
          .noDefault();
    }
  }

  static SpanOperator fromBson(DocumentParser parser) throws BsonParseException {
    var contains = parser.getField(Fields.CONTAINS);
    var first = parser.getField(Fields.FIRST);
    var near = parser.getField(Fields.NEAR);
    var or = parser.getField(Fields.OR);
    var subtract = parser.getField(Fields.SUBTRACT);
    var term = parser.getField(Fields.TERM);

    return parser.getGroup().exactlyOneOf(contains, first, near, or, subtract, term);
  }

  BsonValue spanOperatorToBson();

  /**
   * Obtains all the paths specified by span operators and their nested span operators. Used to
   * determine Lucene field names for these paths when resolving highlights.
   */
  List<StringPath> getPaths();

  @Override
  default BsonValue operatorToBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder();
    return switch (this) {
      case SpanContainsOperator spanContainsOperator ->
          builder.field(Fields.CONTAINS, Optional.of(spanContainsOperator)).build();
      case SpanFirstOperator spanFirstOperator ->
          builder.field(Fields.FIRST, Optional.of(spanFirstOperator)).build();
      case SpanNearOperator spanNearOperator ->
          builder.field(Fields.NEAR, Optional.of(spanNearOperator)).build();
      case SpanOrOperator spanOrOperator ->
          builder.field(Fields.OR, Optional.of(spanOrOperator)).build();
      case SpanSubtractOperator spanSubtractOperator ->
          builder.field(Fields.SUBTRACT, Optional.of(spanSubtractOperator)).build();
      case SpanTermOperator spanTermOperator ->
          builder.field(Fields.TERM, Optional.of(spanTermOperator)).build();
    };
  }
}
