package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.server.command.search.definition.request.BatchOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.GetMoreCommandDefinitionBuilder;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GetMoreCommandDefinitionTest {

  private static final String SUITE_NAME = "get-more";
  private static final BsonDeserializationTestSuite<GetMoreCommandDefinition> TEST_SUITE =
      fromDocument(
          "src/test/unit/resources/server/command/search/definition/request",
          SUITE_NAME,
          GetMoreCommandDefinition::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<GetMoreCommandDefinition> testSpec;

  public GetMoreCommandDefinitionTest(
      BsonDeserializationTestSuite.TestSpecWrapper<GetMoreCommandDefinition> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<BsonDeserializationTestSuite.TestSpecWrapper<GetMoreCommandDefinition>>
      data() {
    return TEST_SUITE.withExamples(cursorId(), cursorOptions(), cursorOptionsUnknownField());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<GetMoreCommandDefinition> cursorId() {
    return TestSpec.valid(
        "cursorId", GetMoreCommandDefinitionBuilder.builder().cursorId(123L).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<GetMoreCommandDefinition> cursorOptions() {
    return TestSpec.valid(
        "cursor-options",
        GetMoreCommandDefinitionBuilder.builder()
            .cursorId(123L)
            .cursorOptions(BatchOptionsDefinitionBuilder.builder().docsRequested(25).build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<GetMoreCommandDefinition>
      cursorOptionsUnknownField() {
    return TestSpec.valid(
        "cursor-options-unknown-field",
        GetMoreCommandDefinitionBuilder.builder()
            .cursorId(123L)
            .cursorOptions(BatchOptionsDefinitionBuilder.builder().docsRequested(25).build())
            .build());
  }
}
