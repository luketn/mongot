package com.xgen.mongot.index.query.scores.expressions;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public sealed interface Expression extends DocumentEncodable
    permits AddExpression,
        ConstantExpression,
        GaussianDecayExpression,
        Log1PExpression,
        LogExpression,
        MultiplyExpression,
        PathExpression,
        ScoreExpression {

  class Fields {
    private static final Field.Optional<AddExpression> ADD =
        Fields.buildWithValueParser("add", AddExpression::fromBson);

    private static final Field.Optional<ConstantExpression> CONSTANT =
        Fields.buildWithValueParser("constant", ConstantExpression::fromBson);

    private static final Field.Optional<GaussianDecayExpression> GAUSS =
        Fields.buildWithValueParser("gauss", GaussianDecayExpression::fromBson);

    private static final Field.Optional<LogExpression> LOG =
        Fields.buildWithDocumentParser("log", LogExpression::fromBson);

    private static final Field.Optional<Log1PExpression> LOG1P =
        Fields.buildWithDocumentParser("log1p", Log1PExpression::fromBson);

    private static final Field.Optional<MultiplyExpression> MULTIPLY =
        Fields.buildWithValueParser("multiply", MultiplyExpression::fromBson);

    private static final Field.Optional<PathExpression> PATH =
        Fields.buildWithValueParser("path", PathExpression::fromBson);

    private static final Field.Optional<ScoreExpression> SCORE =
        Fields.buildWithValueParser("score", ScoreExpression::fromBson);

    // Produces a T from a field's raw BsonValue.
    private static <T extends Expression> Field.Optional<T> buildWithValueParser(
        String name, ClassField.FromValueParser<T> valueParser) {
      return Field.builder(name)
          .classField(valueParser, Expression::expressionToBson)
          .optional()
          .noDefault();
    }

    private static <T extends Expression> Field.Optional<T> buildWithDocumentParser(
        String name, ClassField.FromDocumentParser<T> documentParser) {
      return Field.builder(name)
          .classField(documentParser, Expression::expressionToBson)
          .disallowUnknownFields()
          .optional()
          .noDefault();
    }
  }

  List<FieldPath> getPaths();

  /**
   * Concrete classes should implement this instead of overriding Expression::toBson.
   * Expression::toBson will add the proper field for the concrete expression to a BsonDocument,
   * then delegate to expressionToBson to encode the expression document.
   */
  BsonValue expressionToBson();

  static Expression fromBson(DocumentParser parser) throws BsonParseException {
    return parser
        .getGroup()
        .exactlyOneOf(
            parser.getField(Fields.ADD),
            parser.getField(Fields.CONSTANT),
            parser.getField(Fields.GAUSS),
            parser.getField(Fields.LOG),
            parser.getField(Fields.LOG1P),
            parser.getField(Fields.MULTIPLY),
            parser.getField(Fields.PATH),
            parser.getField(Fields.SCORE));
  }

  @Override
  default BsonDocument toBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder();

    return switch (this) {
      case AddExpression add -> builder.field(Fields.ADD, Optional.of(add)).build();
      case ConstantExpression constant ->
          builder.field(Fields.CONSTANT, Optional.of(constant)).build();
      case GaussianDecayExpression gauss -> builder.field(Fields.GAUSS, Optional.of(gauss)).build();
      case LogExpression log -> builder.field(Fields.LOG, Optional.of(log)).build();
      case Log1PExpression log1p -> builder.field(Fields.LOG1P, Optional.of(log1p)).build();
      case MultiplyExpression multiply ->
          builder.field(Fields.MULTIPLY, Optional.of(multiply)).build();
      case PathExpression path -> builder.field(Fields.PATH, Optional.of(path)).build();
      case ScoreExpression score -> builder.field(Fields.SCORE, Optional.of(score)).build();
    };
  }
}
