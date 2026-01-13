package com.xgen.mongot.config.provider.mongod;

import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record MongodConnectionString(String hostname, String connectionString)
    implements DocumentEncodable {
  static class Fields {
    public static final Field.Required<String> HOSTNAME =
        Field.builder("hostname").stringField().required();
    public static final Field.Required<String> CONNECTION_STRING =
        Field.builder("mongodConnectionString").stringField().required();
  }

  /** Generates a DocumentParser from the json string from the request. */
  public static MongodConnectionString fromJson(String json) throws BsonParseException {
    BsonDocument doc = JsonCodec.fromJson(json);
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      return fromBson(parser);
    }
  }

  /** Deserializes a mongodConnectionString from a DocumentParser. */
  public static MongodConnectionString fromBson(DocumentParser parser) throws BsonParseException {
    return new MongodConnectionString(
        parser.getField(Fields.HOSTNAME).unwrap(),
        parser.getField(Fields.CONNECTION_STRING).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.HOSTNAME, this.hostname)
        .field(Fields.CONNECTION_STRING, this.connectionString)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MongodConnectionString that = (MongodConnectionString) o;
    return this.hostname.equals(that.hostname)
        && this.connectionString.equals(that.connectionString);
  }
}
