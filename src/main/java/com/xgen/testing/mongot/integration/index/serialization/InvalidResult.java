package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public class InvalidResult extends Result {

  static class Fields {
    static final Field.Required<String> ERROR_MESSAGE_CONTAINS =
        Field.builder("errorMessageContains").stringField().required();
  }

  private final String errorMessageContains;

  private InvalidResult(String errorMessageContains) {
    this.errorMessageContains = errorMessageContains;
  }

  static InvalidResult fromBson(DocumentParser parser) throws BsonParseException {
    return new InvalidResult(parser.getField(Fields.ERROR_MESSAGE_CONTAINS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Result.Fields.VALID, false)
        .field(Fields.ERROR_MESSAGE_CONTAINS, this.errorMessageContains)
        .build();
  }

  public String getErrorMessageContains() {
    return this.errorMessageContains;
  }

  @Override
  public Type getType() {
    return Type.INVALID;
  }
}
