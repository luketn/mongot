package com.xgen.testing.mongot.integration.index.serialization;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.CheckReturnValue;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class QueryTestSuite implements DocumentEncodable {
  static class Fields {
    static final Field.Required<List<TestSpec>> TESTS =
        Field.builder("tests")
            .classField(TestSpec::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final List<TestSpec> tests;

  private QueryTestSuite(List<TestSpec> tests) {
    this.tests = tests;
  }

  /** Deserializes the TestSuite from a json string. */
  public static QueryTestSuite fromJson(String json) throws BsonParseException {
    BsonDocument document = JsonCodec.fromJson(json);

    // Expand according to overlay rules. This part is a bit of manual BSON parsing,
    // but I cannot avoid it.
    List<BsonValue> testSpecList = document.getArray(Fields.TESTS.getName());
    Map<String, BsonDocument> testNameToSpec = new HashMap<>();
    for (BsonValue testSpec : testSpecList) {
      var testSpecDoc = testSpec.asDocument();
      String testName = testSpecDoc.getString(TestSpec.Fields.NAME.getName()).toString();
      testNameToSpec.put(testName, testSpecDoc);
    }
    for (int i = 0; i < testSpecList.size(); i++) {
      BsonDocument testSpecDoc = testSpecList.get(i).asDocument();
      if (testSpecDoc.containsKey(TestSpec.Fields.BASED_ON_TEST_SPEC.getName())) {
        String baseSpecName =
            testSpecDoc.getString(TestSpec.Fields.BASED_ON_TEST_SPEC.getName()).toString();
        BsonDocument baseSpecDoc = testNameToSpec.get(baseSpecName);
        checkState(baseSpecDoc != null, "Cannot find existing testSpec with name %s", baseSpecName);
        BsonDocument newDoc = bsonOverlay(baseSpecDoc, testSpecDoc);
        testSpecList.set(i, newDoc);
      }
    }
    document.put(Fields.TESTS.getName(), new BsonArray(testSpecList));

    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return QueryTestSuite.fromBson(parser);
    }
  }

  private static QueryTestSuite fromBson(DocumentParser parser) throws BsonParseException {
    return new QueryTestSuite(parser.getField(Fields.TESTS).unwrap());
  }

  /**
   * The overlay rule is: if one field exists only in one of the base or overlay docs, return it. If
   * both docs have one field, then use the doc from the overlay doc. One special rule is when both
   * docs have one document field, recursively call this method. This is a poor man's version of
   * https://jsonnet.org/.
   */
  @CheckReturnValue
  private static BsonDocument bsonOverlay(BsonDocument base, BsonDocument overlay) {
    BsonDocument result = new BsonDocument();
    for (String key : base.keySet()) {
      if (!overlay.containsKey(key)) {
        result.put(key, base.get(key));
        continue;
      }
      if (base.get(key).isDocument() && overlay.get(key).isDocument()) {
        result.put(key, bsonOverlay(base.getDocument(key), overlay.getDocument(key)));
      } else {
        result.put(key, overlay.get(key));
      }
    }
    for (String key : overlay.keySet()) {
      if (!base.containsKey(key)) {
        result.put(key, overlay.get(key));
      }
    }
    return result;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.TESTS, this.tests).build();
  }

  public List<TestSpec> getTests() {
    return this.tests;
  }
}
