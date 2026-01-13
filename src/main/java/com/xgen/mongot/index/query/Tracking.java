package com.xgen.mongot.index.query;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record Tracking(String searchTerms) implements DocumentEncodable {

  static class Fields {
    static final Field.Required<String> SEARCH_TERMS =
        Field.builder("searchTerms").stringField().required();
  }

  static Tracking fromBson(DocumentParser parser) throws BsonParseException {
    return new Tracking(parser.getField(Fields.SEARCH_TERMS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.SEARCH_TERMS, this.searchTerms).build();
  }
}
