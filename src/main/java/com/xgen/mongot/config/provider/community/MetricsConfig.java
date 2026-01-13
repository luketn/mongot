package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record MetricsConfig(boolean enabled, String address) implements DocumentEncodable {
  private static class Fields {
    public static final Field.Required<Boolean> ENABLED =
        Field.builder("enabled").booleanField().required();
    public static final Field.WithDefault<String> ADDRESS =
        Field.builder("address").stringField().optional().withDefault("localhost:9946");
  }

  public static MetricsConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new MetricsConfig(
        parser.getField(Fields.ENABLED).unwrap(), parser.getField(Fields.ADDRESS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENABLED, this.enabled)
        .field(Fields.ADDRESS, this.address)
        .build();
  }
}
