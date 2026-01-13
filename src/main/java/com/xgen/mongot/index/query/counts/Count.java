package com.xgen.mongot.index.query.counts;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record Count(Type type, int threshold) implements DocumentEncodable {

  private static class Fields {
    private static final Field.WithDefault<Type> TYPE =
        Field.builder("type")
            .enumField(Type.class)
            .asCamelCase()
            .optional()
            .withDefault(Type.LOWER_BOUND);

    private static final Field.WithDefault<Integer> THRESHOLD =
        Field.builder("threshold").intField().mustBeNonNegative().optional().withDefault(1000);
  }

  public static final Count DEFAULT =
      new Count(Fields.TYPE.getDefaultValue(), Fields.THRESHOLD.getDefaultValue());

  public enum Type {
    LOWER_BOUND,
    TOTAL
  }

  public static Count fromBson(DocumentParser parser) throws BsonParseException {
    return new Count(
        parser.getField(Fields.TYPE).unwrap(), parser.getField(Fields.THRESHOLD).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TYPE, this.type)
        .field(Fields.THRESHOLD, this.threshold)
        .build();
  }
}
