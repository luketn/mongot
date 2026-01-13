package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinitionBuilder;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PlanShardedSearchDefinitionTest {

  private static final String SUITE_NAME = "plan-sharded-search";
  private static final BsonDeserializationTestSuite<PlanShardedSearchCommandDefinition> TEST_SUITE =
      fromDocument(
          "src/test/unit/resources/server/command/search/definition/request",
          SUITE_NAME,
          PlanShardedSearchCommandDefinition::fromBson);

  private final TestSpecWrapper<PlanShardedSearchCommandDefinition> testSpec;

  public PlanShardedSearchDefinitionTest(
      TestSpecWrapper<PlanShardedSearchCommandDefinition> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<PlanShardedSearchCommandDefinition>> data() {
    return TEST_SUITE.withExamples(simple(), simpleOnView(), unknownField());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<PlanShardedSearchCommandDefinition>
      simple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "simple",
        PlanShardedSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .searchFeatures(new PlanShardedSearchCommandDefinition.SearchFeatures(1))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<PlanShardedSearchCommandDefinition>
      simpleOnView() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "simple-on-view",
        PlanShardedSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .viewName("my-view")
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .searchFeatures(new PlanShardedSearchCommandDefinition.SearchFeatures(1))
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<PlanShardedSearchCommandDefinition>
      unknownField() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "unknownField",
        PlanShardedSearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .searchFeatures(new PlanShardedSearchCommandDefinition.SearchFeatures(1))
            .build());
  }
}
