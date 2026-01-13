package com.xgen.mongot.index;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/** Contains meta info for the query. */
public record Variables(MetaResults metaResults) implements DocumentEncodable {

  public static class Fields {
    static final Field.Required<MetaResults> SEARCH_META =
        Field.builder("SEARCH_META")
            .classField(MetaResults::fromBson)
            .disallowUnknownFields()
            .required();
  }

  public static Variables fromBson(DocumentParser parser) throws BsonParseException {
    return new Variables(parser.getField(Fields.SEARCH_META).unwrap());
  }

  public static Variables fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.SEARCH_META, this.metaResults).build();
  }
}
