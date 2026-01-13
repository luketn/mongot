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

public class VectorIndexQueryTestSuite implements DocumentEncodable {
  static class Fields {
    static final Field.Required<List<VectorIndexQueryTestSpec>> TESTS =
        Field.builder("tests")
            .classField(VectorIndexQueryTestSpec::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final List<VectorIndexQueryTestSpec> tests;

  private VectorIndexQueryTestSuite(List<VectorIndexQueryTestSpec> tests) {
    this.tests = tests;
  }

  /** Deserializes the TestSuite from a json string. */
  public static VectorIndexQueryTestSuite fromJson(String json) throws BsonParseException {
    BsonDocument document = JsonCodec.fromJson(json);
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return VectorIndexQueryTestSuite.fromBson(parser);
    }
  }

  private static VectorIndexQueryTestSuite fromBson(DocumentParser parser)
      throws BsonParseException {
    return new VectorIndexQueryTestSuite(parser.getField(Fields.TESTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.TESTS, this.tests).build();
  }

  public List<VectorIndexQueryTestSpec> getTests() {
    return this.tests;
  }
}
