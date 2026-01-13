package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public class ExpectedSearchHighlightText implements DocumentEncodable {
  static class Fields {
    static final Field.Required<String> TYPE = Field.builder("type").stringField().required();

    static final Field.Required<String> VALUE = Field.builder("value").stringField().required();
  }

  private final String type;
  private final String value;

  private ExpectedSearchHighlightText(String type, String value) {
    this.type = type;
    this.value = value;
  }

  static ExpectedSearchHighlightText fromBson(DocumentParser parser) throws BsonParseException {
    return new ExpectedSearchHighlightText(
        parser.getField(Fields.TYPE).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TYPE, this.type)
        .field(Fields.VALUE, this.value)
        .build();
  }

  @Override
  public String toString() {
    return "ExpectedSearchHighlightText[type=" + this.type + ", value=" + this.value + "]";
  }

  public String getType() {
    return this.type;
  }

  public String getValue() {
    return this.value;
  }
}
