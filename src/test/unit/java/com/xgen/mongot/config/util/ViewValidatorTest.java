package com.xgen.mongot.config.util;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.definition.InvalidViewDefinitionException;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ViewValidatorTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<TestSpec> data() throws Exception {
    var json =
        Files.readString(Paths.get("src/test/unit/resources/config/util/viewValidation.json"));
    return TestSuite.fromJson(json).getTests();
  }

  private final TestSpec spec;

  public ViewValidatorTest(TestSpec spec) {
    this.spec = spec;
  }

  @Test
  public void test() throws InvalidViewDefinitionException {

    if (this.spec.result.valid) {
      Check.isEmpty(this.spec.result.errorMessageContains, "errorMessageContains");
      ViewValidator.validate(ViewDefinition.existing("test", this.spec.viewPipeline));
    } else {
      InvalidViewDefinitionException exception =
          Assert.assertThrows(
              InvalidViewDefinitionException.class,
              () ->
                  ViewValidator.validate(ViewDefinition.existing("test", this.spec.viewPipeline)));

      this.spec.result.errorMessageContains.ifPresent(
          substring -> Truth.assertThat(exception).hasMessageThat().contains(substring));
    }
  }

  public static class TestSuite {

    static class Fields {
      static final Field.Required<List<TestSpec>> TESTS =
          Field.builder("tests")
              .classField(TestSpec::fromBson)
              .disallowUnknownFields()
              .asList()
              .required();
    }

    private final List<TestSpec> tests;

    private TestSuite(List<TestSpec> tests) {
      this.tests = tests;
    }

    public static TestSuite fromJson(String json) throws BsonParseException {
      BsonDocument document = JsonCodec.fromJson(json);
      try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
        return TestSuite.fromBson(parser);
      }
    }

    private static TestSuite fromBson(DocumentParser parser) throws BsonParseException {
      return new TestSuite(parser.getField(TestSuite.Fields.TESTS).unwrap());
    }

    public List<TestSpec> getTests() {
      return this.tests;
    }
  }

  public static class TestSpec implements Encodable {

    private static class Fields {
      static final Field.Required<String> NAME = Field.builder("name").stringField().required();

      static final Field.Optional<String> DESCRIPTION =
          Field.builder("description").stringField().optional().noDefault();

      static final Field.Required<List<BsonDocument>> VIEW_PIPELINE =
          Field.builder("viewPipeline").documentField().asList().required();

      static final Field.Required<TestResult> RESULT =
          Field.builder("result")
              .classField(TestResult::fromBson)
              .disallowUnknownFields()
              .required();
    }

    private final String name;

    @SuppressWarnings("unused")
    private final Optional<String> description;

    private final List<BsonDocument> viewPipeline;
    private final TestResult result;

    public TestSpec(
        String name,
        Optional<String> description,
        List<BsonDocument> viewPipeline,
        TestResult result) {
      this.description = description;
      this.name = name;
      this.viewPipeline = viewPipeline;
      this.result = result;
    }

    public static TestSpec fromBson(DocumentParser parser) throws BsonParseException {
      return new TestSpec(
          parser.getField(TestSpec.Fields.NAME).unwrap(),
          parser.getField(TestSpec.Fields.DESCRIPTION).unwrap(),
          parser.getField(TestSpec.Fields.VIEW_PIPELINE).unwrap(),
          parser.getField(TestSpec.Fields.RESULT).unwrap());
    }

    @Override
    public BsonValue toBson() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  public static class TestResult implements Encodable {

    private static class Fields {
      static final Field.Required<Boolean> VALID = Field.builder("valid").booleanField().required();

      static final Field.Optional<String> ERROR_MESSAGE_CONTAINS =
          Field.builder("errorMessageContains").stringField().optional().noDefault();
    }

    private final boolean valid;
    private final Optional<String> errorMessageContains;

    public TestResult(boolean valid, Optional<String> errorMessageContains) {
      this.valid = valid;
      this.errorMessageContains = errorMessageContains;
    }

    public static TestResult fromBson(DocumentParser parser) throws BsonParseException {
      return new TestResult(
          parser.getField(Fields.VALID).unwrap(),
          parser.getField(Fields.ERROR_MESSAGE_CONTAINS).unwrap());
    }

    @Override
    public BsonValue toBson() {
      throw new UnsupportedOperationException();
    }
  }
}
