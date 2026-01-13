package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public class MqlFilterTestSuite implements DocumentEncodable {
  static class Fields {
    static final Field.Required<List<MqlFilterTestSpec>> TESTS =
        Field.builder("tests")
            .classField(MqlFilterTestSpec::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final List<MqlFilterTestSpec> tests;

  private MqlFilterTestSuite(List<MqlFilterTestSpec> tests) {
    this.tests = tests;
  }

  public static MqlFilterTestSuite fromJson(String json) throws BsonParseException {
    BsonDocument document = JsonCodec.fromJson(json);
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return MqlFilterTestSuite.fromBson(parser);
    }
  }

  private static MqlFilterTestSuite fromBson(DocumentParser parser) throws BsonParseException {
    return new MqlFilterTestSuite(parser.getField(Fields.TESTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.TESTS, this.tests).build();
  }

  public List<MqlFilterTestSpec> getTests() {
    return this.tests;
  }
}
