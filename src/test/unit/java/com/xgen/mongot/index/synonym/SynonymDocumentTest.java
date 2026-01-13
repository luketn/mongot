package com.xgen.mongot.index.synonym;

import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.synonym.SynonymDocumentBuilder;
import java.util.List;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SynonymDocumentTest.TestDeserialization.class,
      SynonymDocumentTest.TestSerialization.class
    })
public class SynonymDocumentTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "synonym-document-deserialization";

    private static final BsonDeserializationTestSuite<SynonymDocument> TEST_SUITE =
        BsonDeserializationTestSuite.fromValue(
            "src/test/unit/resources/index/synonym/",
            SUITE_NAME,
            SynonymDocumentTest.TestDeserialization::fromBsonExceptionUnwrapped);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SynonymDocument> testSpec;

    /**
     * Make a wrapper for deserialization to
     *
     * <p>1) Enter through the "front door, {@link SynonymDocument#fromBson(BsonDocument)} - _not_
     * {@code SynonymDocument#fromBson(DocumentParser)}. Method taking DocumentParser argument may
     * not be configured to allow unknown fields on the parser, which doesn't let us test that
     * SynonymDocument is configured that way.
     *
     * <p>2) {@link SynonymDocument} parsing wraps {@link BsonParseException}s in {@link
     * SynonymMappingException}s, and {@link BsonDeserializationTestSuite} expects to handle
     * BsonParseExceptions. Unwrap them here to present exceptions as expected by the test suite.
     */
    private static SynonymDocument fromBsonExceptionUnwrapped(
        BsonParseContext context, BsonValue document) throws BsonParseException {
      if (document.getBsonType() != BsonType.DOCUMENT) {
        Assert.fail("did not deserialize to document");
      }
      try {
        return SynonymDocument.fromTestBson(document.asDocument());
      } catch (SynonymMappingException e) {
        if (e.getCause() instanceof BsonParseException) {
          throw (BsonParseException) e.getCause();
        }

        throw new AssertionError("cause should always be BsonParseException", e);
      }
    }

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<SynonymDocument> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SynonymDocument>> data() {
      return TEST_SUITE.withExamples(
          equivalent(),
          explicit(),
          equivalentUnvalidatedInput(),
          explicitAllowsOtherFields(),
          equivalentNumericDocId(),
          equivalentStringDocId(),
          equivalentObjectIdDocId(),
          equivalentUuidDocId());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument> equivalent() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "equivalent", SynonymDocumentBuilder.equivalent(List.of("car", "truck")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument> explicit() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit", SynonymDocumentBuilder.explicit(List.of("vehicle"), List.of("car", "truck")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument>
        equivalentUnvalidatedInput() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "equivalent has unvalidated input field",
          SynonymDocumentBuilder.equivalent(List.of("car", "truck")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument>
        explicitAllowsOtherFields() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit allows other fields",
          SynonymDocumentBuilder.explicit(List.of("vehicle", "jalopy"), List.of("car", "truck")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument>
        equivalentNumericDocId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "equivalent with numeric docId",
          SynonymDocumentBuilder.equivalent(List.of("car", "truck"), new BsonInt32(12345)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument> equivalentStringDocId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "equivalent with string docId",
          SynonymDocumentBuilder.equivalent(List.of("car", "truck"), new BsonString("12345")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument>
        equivalentObjectIdDocId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "equivalent with objectId docId",
          SynonymDocumentBuilder.equivalent(
              List.of("car", "truck"), new BsonObjectId(new ObjectId("507f191e810c19729de860ea"))));
    }

    private static BsonDeserializationTestSuite.ValidSpec<SynonymDocument> equivalentUuidDocId() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "equivalent with uuid docId",
          SynonymDocumentBuilder.equivalent(
              List.of("car", "truck"),
              new BsonBinary(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))));
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "synonym-document-serialization";
    private static final BsonSerializationTestSuite<SynonymDocument> TEST_SUITE =
        fromEncodable("src/test/unit/resources/index/synonym/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<SynonymDocument> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<SynonymDocument> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SynonymDocument>> data() {
      return List.of(equivalent(), explicit());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SynonymDocument> equivalent() {
      return BsonSerializationTestSuite.TestSpec.create(
          "equivalent", SynonymDocumentBuilder.equivalent(List.of("car", "truck")));
    }

    private static BsonSerializationTestSuite.TestSpec<SynonymDocument> explicit() {
      return BsonSerializationTestSuite.TestSpec.create(
          "explicit",
          SynonymDocumentBuilder.explicit(
              List.of("vehicle", "transportation"), List.of("car", "truck")));
    }
  }
}
