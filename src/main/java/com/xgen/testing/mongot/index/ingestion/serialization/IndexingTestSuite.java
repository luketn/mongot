package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

public class IndexingTestSuite implements DocumentEncodable {
  static class Fields {
    static final Field.Required<List<BsonProcessorTestSpec>> TESTS =
        Field.builder("tests")
            .classField(BsonProcessorTestSpec::fromBson)
            .allowUnknownFields()
            .asList()
            .required();
  }

  private final List<BsonProcessorTestSpec> tests;

  public IndexingTestSuite(List<BsonProcessorTestSpec> tests) {
    this.tests = tests;
  }

  public static IndexingTestSuite fromJson(String json) throws BsonParseException {
    BsonDocument document = JsonCodec.fromJson(json);
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return IndexingTestSuite.fromBson(parser);
    }
  }

  private static IndexingTestSuite fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexingTestSuite(parser.getField(Fields.TESTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.TESTS, this.tests).build();
  }

  public List<BsonProcessorTestSpec> getTests() {
    return this.tests;
  }
}
