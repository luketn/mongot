package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.SearchOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SearchOperatorTest {

  private static final String SUITE_NAME = "search";
  private static final BsonDeserializationTestSuite<SearchOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SearchOperator::fromBson);

  private final TestSpecWrapper<SearchOperator> testSpec;

  public SearchOperatorTest(TestSpecWrapper<SearchOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SearchOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(),
        multiPath(),
        withBoostScore(),
        defaultPhrase(),
        explicitPhraseOptions(),
        maxExpansions0(),
        maxExpansions1000(),
        slop0(),
        slop100(),
        phraseNullIsOff());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SearchOperator> simple() {
    return TestSpec.valid(
        "simple", OperatorBuilder.search().path("title").query("godfather").build());
  }

  private static ValidSpec<SearchOperator> multiPath() {
    return TestSpec.valid(
        "multi path",
        OperatorBuilder.search()
            .path(StringPathBuilder.withMulti("title", "my-multi"))
            .query("godfather")
            .build());
  }

  private static ValidSpec<SearchOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<SearchOperator> defaultPhrase() {
    return TestSpec.valid(
        "default phrase",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .phrase(SearchOperatorBuilder.phraseBuilder().build())
            .build());
  }

  private static ValidSpec<SearchOperator> explicitPhraseOptions() {
    return TestSpec.valid(
        "explicit phrase options",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .phrase(
                SearchOperatorBuilder.phraseBuilder().maxExpansions(1).prefix(true).slop(3).build())
            .build());
  }

  private static ValidSpec<SearchOperator> maxExpansions0() {
    return TestSpec.valid(
        "maxExpansions 0",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .phrase(SearchOperatorBuilder.phraseBuilder().maxExpansions(0).prefix(true).build())
            .build());
  }

  private static ValidSpec<SearchOperator> maxExpansions1000() {
    return TestSpec.valid(
        "maxExpansions 1000",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .phrase(SearchOperatorBuilder.phraseBuilder().maxExpansions(1000).prefix(true).build())
            .build());
  }

  private static ValidSpec<SearchOperator> slop0() {
    return TestSpec.valid(
        "slop 0",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .phrase(SearchOperatorBuilder.phraseBuilder().slop(0).build())
            .build());
  }

  private static ValidSpec<SearchOperator> slop100() {
    return TestSpec.valid(
        "slop 100",
        OperatorBuilder.search()
            .path("title")
            .query("godfather")
            .phrase(SearchOperatorBuilder.phraseBuilder().slop(100).build())
            .build());
  }

  private static ValidSpec<SearchOperator> phraseNullIsOff() {
    return TestSpec.valid(
        "phrase null is off", OperatorBuilder.search().path("title").query("godfather").build());
  }
}
