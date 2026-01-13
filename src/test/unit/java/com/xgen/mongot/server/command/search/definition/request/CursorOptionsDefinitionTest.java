package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      CursorOptionsDefinitionTest.TestDeserialization.class,
      CursorOptionsDefinitionTest.TestSerialization.class,
    })
public class CursorOptionsDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "cursor-options-deserialization";
    private static final BsonDeserializationTestSuite<CursorOptionsDefinition> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/server/command/search/definition/request",
            SUITE_NAME,
            CursorOptionsDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<CursorOptionsDefinition> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<CursorOptionsDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<CursorOptionsDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          simpleDocsRequested(), simpleBatchSize(), missingAllParameters());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<CursorOptionsDefinition>
        simpleDocsRequested() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simpleDocsRequested",
          CursorOptionsDefinitionBuilder.builder().docsRequested(25).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CursorOptionsDefinition>
        simpleBatchSize() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simpleBatchSize", CursorOptionsDefinitionBuilder.builder().batchSize(25).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CursorOptionsDefinition>
        missingAllParameters() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "missing-all-parameters", CursorOptionsDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "cursor-options-serialization";
    private static final BsonSerializationTestSuite<CursorOptionsDefinition> TEST_SUITE =
        fromEncodable(
            "src/test/unit/resources/server/command/search/definition/request", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<CursorOptionsDefinition> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<CursorOptionsDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<CursorOptionsDefinition>> data() {
      return List.of(simpleDocsRequested(), simpleBatchSize(), tokens());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<CursorOptionsDefinition>
        simpleDocsRequested() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simpleDocsRequested",
          CursorOptionsDefinitionBuilder.builder().docsRequested(25).build());
    }

    private static BsonSerializationTestSuite.TestSpec<CursorOptionsDefinition> simpleBatchSize() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simpleBatchSize", CursorOptionsDefinitionBuilder.builder().batchSize(25).build());
    }

    private static BsonSerializationTestSuite.TestSpec<CursorOptionsDefinition> tokens() {
      return BsonSerializationTestSuite.TestSpec.create(
          "tokens",
          CursorOptionsDefinitionBuilder.builder()
              .docsRequested(25)
              .requireSequenceTokens(true)
              .build());
    }
  }
}
