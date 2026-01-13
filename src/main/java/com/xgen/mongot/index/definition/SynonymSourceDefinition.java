package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record SynonymSourceDefinition(String collection) implements DocumentEncodable {
  public static class Fields {
    // Collection name.
    static final Field.Required<String> COLLECTION =
        Field.builder("collection").stringField().mustNotBeEmpty().required();
  }

  static SynonymSourceDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new SynonymSourceDefinition(parser.getField(Fields.COLLECTION).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.COLLECTION, this.collection).build();
  }
}
