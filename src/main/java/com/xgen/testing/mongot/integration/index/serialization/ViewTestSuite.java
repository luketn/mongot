package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public class ViewTestSuite {

  static class Fields {
    static final Field.Required<List<ViewTestSpec>> TESTS =
        Field.builder("tests")
            .classField(ViewTestSpec::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final List<ViewTestSpec> tests;

  private ViewTestSuite(List<ViewTestSpec> tests) {
    this.tests = tests;
  }

  public static ViewTestSuite fromJson(String json) throws BsonParseException {
    BsonDocument document = JsonCodec.fromJson(json);
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return ViewTestSuite.fromBson(parser);
    }
  }

  private static ViewTestSuite fromBson(DocumentParser parser) throws BsonParseException {
    return new ViewTestSuite(parser.getField(Fields.TESTS).unwrap());
  }

  public List<ViewTestSpec> getTests() {
    return this.tests;
  }
}
