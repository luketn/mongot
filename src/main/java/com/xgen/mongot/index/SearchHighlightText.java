package com.xgen.mongot.index;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record SearchHighlightText(String value, Type type) implements DocumentEncodable {

  private static class Fields {
    private static final Field.Required<String> VALUE =
        Field.builder("value").stringField().required();

    private static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();
  }

  public enum Type {
    HIT("hit"),
    TEXT("text");

    private final String type;

    Type(String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return this.type;
    }
  }

  static SearchHighlightText fromBson(DocumentParser parser) throws BsonParseException {
    return new SearchHighlightText(
        parser.getField(Fields.VALUE).unwrap(), parser.getField(Fields.TYPE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.VALUE, this.value)
        .field(Fields.TYPE, this.type)
        .build();
  }
}
