package com.xgen.mongot.server.command.management.definition.common;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/**
 * A representation of a search index by its name and ID. Used in the response of
 * CreateSearchIndexes.
 */
public record NamedSearchIndexId(String name, String id) implements DocumentEncodable {
  public static class Fields {
    public static final Field.Required<String> NAME =
        Field.builder("name").stringField().required();

    public static final Field.Required<String> ID = Field.builder("id").stringField().required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.ID, this.id)
        .build();
  }

  public static NamedSearchIndexId fromBson(DocumentParser parser) throws BsonParseException {
    return new NamedSearchIndexId(
        parser.getField(Fields.NAME).unwrap(), parser.getField(Fields.ID).unwrap());
  }
}
