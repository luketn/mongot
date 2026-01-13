package com.xgen.mongot.index;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public record FacetInfo(List<FacetBucket> buckets) implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<List<FacetBucket>> BUCKETS =
        Field.builder("buckets")
            .classField(FacetBucket::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  public static FacetInfo fromBson(DocumentParser parser) throws BsonParseException {
    return new FacetInfo(parser.getField(Fields.BUCKETS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.BUCKETS, this.buckets).build();
  }
}
