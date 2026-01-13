package com.xgen.mongot.server.command.search.definition.request;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public record GetMoreCommandDefinition(
    long cursorId, Optional<BatchOptionsDefinition> cursorOptions) {
  static class Fields {
    static final Field.Required<Long> CURSOR_ID =
        Field.builder(NAME).longField().mustBePositive().required();

    static final Field.Optional<BatchOptionsDefinition> CURSOR_OPTIONS =
        Field.builder("cursorOptions")
            .classField(BatchOptionsDefinition::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  public static final String NAME = "getMore";

  public static GetMoreCommandDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new GetMoreCommandDefinition(
        parser.getField(Fields.CURSOR_ID).unwrap(),
        parser.getField(Fields.CURSOR_OPTIONS).unwrap());
  }

  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CURSOR_ID, this.cursorId)
        .field(Fields.CURSOR_OPTIONS, this.cursorOptions)
        .build();
  }
}
