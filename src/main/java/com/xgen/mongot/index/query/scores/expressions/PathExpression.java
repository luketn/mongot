package com.xgen.mongot.index.query.scores.expressions;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

public record PathExpression(FieldPath path, double undefined) implements Expression {

  private static final String EXPECTED_TYPE = "document or string";

  private static class Fields {
    private static final Field.Required<String> VALUE =
        Field.builder("value").stringField().required();

    private static final Field.WithDefault<Double> UNDEFINED =
        Field.builder("undefined").doubleField().mustBeFinite().optional().withDefault(0.0d);
  }

  /** Constructs a PathExpression from the supplied BsonValue. */
  public static PathExpression fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    switch (bsonValue.getBsonType()) {
      case STRING:
        return new PathExpression(FieldPath.parse(bsonValue.asString().getValue()), 0.0d);

      case DOCUMENT:
        try (var parser = BsonDocumentParser.withContext(context, bsonValue.asDocument()).build()) {
          return new PathExpression(
              FieldPath.parse(parser.getField(Fields.VALUE).unwrap()),
              parser.getField(Fields.UNDEFINED).unwrap());
        }

      default:
        return context.handleUnexpectedType(EXPECTED_TYPE, bsonValue.getBsonType());
    }
  }

  @Override
  public List<FieldPath> getPaths() {
    return List.of(this.path);
  }

  @Override
  public BsonValue expressionToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VALUE, this.path.toString())
        .field(Fields.UNDEFINED, this.undefined)
        .build();
  }
}
