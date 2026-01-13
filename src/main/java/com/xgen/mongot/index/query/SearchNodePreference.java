package com.xgen.mongot.index.query;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

/**
 * SearchNodePreference is a field in the query syntax that the user can optionally provide. The
 * user provided value is used in search-envoy for routing purposes. mongot only deserializes it to
 * ensure syntax correctness and to surface errors to the user if any. This field has no use in
 * query execution within mongot.
 */
public record SearchNodePreference(String key) implements Encodable {

  static final class Fields {
    static final Field.Required<String> KEY =
        Field.builder("key").stringField().mustNotBeEmpty().required();
  }

  @Override
  public BsonValue toBson() {
    return BsonDocumentBuilder.builder().field(Fields.KEY, this.key).build();
  }

  public static SearchNodePreference fromBson(DocumentParser parser) throws BsonParseException {
    return new SearchNodePreference(parser.getField(Fields.KEY).unwrap());
  }
}
