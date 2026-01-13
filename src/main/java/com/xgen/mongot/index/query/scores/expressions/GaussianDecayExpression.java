package com.xgen.mongot.index.query.scores.expressions;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record GaussianDecayExpression(
    PathExpression path, double origin, double scale, double offset, double decay)
    implements Expression {

  @VisibleForTesting
  public static class Fields {
    private static final Field.Required<PathExpression> PATH =
        Field.builder("path")
            .classField(PathExpression::fromBson, Expression::expressionToBson)
            .required();

    private static final Field.Required<Double> ORIGIN =
        Field.builder("origin").doubleField().mustBeFinite().required();

    private static final Field.Required<Double> SCALE =
        Field.builder("scale")
            .doubleField()
            .mustBeFinite()
            .validate(num -> num == 0 ? Optional.of("must be nonzero") : Optional.empty())
            .required();

    public static final Field.WithDefault<Double> OFFSET =
        Field.builder("offset").doubleField().mustBeFinite().optional().withDefault(0.0);

    public static final Field.WithDefault<Double> DECAY =
        Field.builder("decay")
            .doubleField()
            .mustBeFinite()
            .validate(
                num ->
                    (num <= 0 || num >= 1)
                        ? Optional.of("must be between 0 and 1 (exclusive)")
                        : Optional.empty())
            .optional()
            .withDefault(0.5);
  }

  public static GaussianDecayExpression fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    try (var parser = BsonDocumentParser.withContext(context, bsonValue.asDocument()).build()) {
      return new GaussianDecayExpression(
          parser.getField(Fields.PATH).unwrap(),
          parser.getField(Fields.ORIGIN).unwrap(),
          parser.getField(Fields.SCALE).unwrap(),
          parser.getField(Fields.OFFSET).unwrap(),
          parser.getField(Fields.DECAY).unwrap());
    }
  }

  @Override
  public List<FieldPath> getPaths() {
    return List.of(this.path.path());
  }

  @Override
  public BsonValue expressionToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.ORIGIN, this.origin)
        .field(Fields.SCALE, this.scale)
        .field(Fields.OFFSET, this.offset)
        .field(Fields.DECAY, this.decay)
        .build();
  }
}
