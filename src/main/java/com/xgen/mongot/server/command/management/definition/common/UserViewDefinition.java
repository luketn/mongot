package com.xgen.mongot.server.command.management.definition.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

public record UserViewDefinition(String name, Optional<List<BsonDocument>> effectivePipeline)
    implements DocumentEncodable {

  private static class Fields {
    static final Field.Required<String> NAME = Field.builder("name").stringField().required();
    static final Field.Optional<List<BsonDocument>> EFFECTIVE_PIPELINE =
        Field.builder("effectivePipeline")
            .listOf(Value.builder().documentValue().required())
            .optional()
            .noDefault();
  }

  public static UserViewDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new UserViewDefinition(
        parser.getField(Fields.NAME).unwrap(), parser.getField(Fields.EFFECTIVE_PIPELINE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.EFFECTIVE_PIPELINE, this.effectivePipeline)
        .build();
  }
}
