package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record HealthCheckConfig(String address) implements DocumentEncodable {
  private static class Fields {
    public static final Field.WithDefault<String> ADDRESS =
        Field.builder("address").stringField().optional().withDefault("localhost:8080");
  }

  public static HealthCheckConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new HealthCheckConfig(parser.getField(Fields.ADDRESS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.ADDRESS, this.address).build();
  }
}
