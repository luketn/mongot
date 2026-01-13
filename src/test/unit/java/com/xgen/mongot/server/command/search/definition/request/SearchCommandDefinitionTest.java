package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.server.command.search.definition.request.CursorOptionsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.ExplainDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.OptimizationFlagsDefinitionBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.SearchCommandDefinitionBuilder;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SearchCommandDefinitionTest {

  private static final String SUITE_NAME = "search";
  private static final BsonDeserializationTestSuite<SearchCommandDefinition> TEST_SUITE =
      fromDocument(
          "src/test/unit/resources/server/command/search/definition/request",
          SUITE_NAME,
          SearchCommandDefinition::fromBson);

  private final TestSpecWrapper<SearchCommandDefinition> testSpec;

  public SearchCommandDefinitionTest(TestSpecWrapper<SearchCommandDefinition> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SearchCommandDefinition>> data() {
    return TEST_SUITE.withExamples(
        search(),
        searchBeta(),
        searchOnView(),
        searchWithExplain(),
        intermediate(),
        searchWithCursorOptions(),
        searchWithCursorOptionsUnknownField(),
        searchWithOptimizationFlags(),
        searchWithOptimizationFlagsUnknownField());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SearchCommandDefinition> search() {
    return TestSpec.valid(
        "search",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchBeta() {
    return TestSpec.valid(
        "searchBeta",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchOnView() {
    return TestSpec.valid(
        "search-on-view",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .viewName("my-view")
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchWithExplain() {
    return TestSpec.valid(
        "search-with-explain",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .explain(
                ExplainDefinitionBuilder.builder()
                    .verbosity(Explain.Verbosity.ALL_PLANS_EXECUTION)
                    .build())
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> intermediate() {
    return TestSpec.valid(
        "intermediate",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .intermediate(1)
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchWithCursorOptions() {
    return TestSpec.valid(
        "search-with-cursor-options",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .cursorOptions(CursorOptionsDefinitionBuilder.builder().docsRequested(25).build())
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchWithCursorOptionsUnknownField() {
    return TestSpec.valid(
        "search-with-cursor-options-unknown-field",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .cursorOptions(CursorOptionsDefinitionBuilder.builder().docsRequested(25).build())
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchWithOptimizationFlags() {
    return TestSpec.valid(
        "search-with-optimization-flags",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .optimizationFlags(
                OptimizationFlagsDefinitionBuilder.builder()
                    .omitSearchDocumentResults(true)
                    .build())
            .build());
  }

  private static ValidSpec<SearchCommandDefinition> searchWithOptimizationFlagsUnknownField() {
    return TestSpec.valid(
        "search-with-optimization-flags-unknown-field",
        SearchCommandDefinitionBuilder.builder()
            .collectionName("my-collection")
            .db("my-database")
            .collectionUuid(UUID.fromString("522cdf5e-54fc-4230-9d45-49da990e8ea7"))
            .query(
                new BsonDocument()
                    .append(
                        "text",
                        new BsonDocument()
                            .append("path", new BsonString("title"))
                            .append("query", new BsonString("godfather"))))
            .optimizationFlags(
                OptimizationFlagsDefinitionBuilder.builder()
                    .omitSearchDocumentResults(true)
                    .build())
            .build());
  }
}
