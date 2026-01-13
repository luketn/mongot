package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TermOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TermOperatorTest {

  private static final String SUITE_NAME = "term";
  private static final BsonDeserializationTestSuite<TermOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, TermOperator::fromBson);

  private final TestSpecWrapper<TermOperator> testSpec;

  public TermOperatorTest(TestSpecWrapper<TermOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<TermOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(),
        multiPath(),
        withBoostScore(),
        defaultFuzzy(),
        explicitFuzzyOptions(),
        fuzzyNullIsOff(),
        prefixTrue(),
        regexTrue(),
        wildcardTrue(),
        fuzzyAndExplicitFalseOptionsNotMutuallyExclusive());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<TermOperator> simple() {
    return TestSpec.valid(
        "simple", OperatorBuilder.term().path("title").query("godfather").build());
  }

  private static ValidSpec<TermOperator> multiPath() {
    return TestSpec.valid(
        "multi path",
        OperatorBuilder.term()
            .path(StringPathBuilder.withMulti("title", "my-multi"))
            .query("godfather")
            .build());
  }

  private static ValidSpec<TermOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.term()
            .path("title")
            .query("godfather")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<TermOperator> defaultFuzzy() {
    return TestSpec.valid(
        "default fuzzy",
        OperatorBuilder.term()
            .path("title")
            .query("godfather")
            .fuzzy(TermOperatorBuilder.fuzzyBuilder().build())
            .build());
  }

  private static ValidSpec<TermOperator> explicitFuzzyOptions() {
    return TestSpec.valid(
        "explicit fuzzy options",
        OperatorBuilder.term()
            .path("title")
            .query("godfather")
            .fuzzy(
                TermOperatorBuilder.fuzzyBuilder()
                    .maxEdits(1)
                    .maxExpansions(2)
                    .prefixLength(3)
                    .build())
            .build());
  }

  private static ValidSpec<TermOperator> fuzzyNullIsOff() {
    return TestSpec.valid(
        "fuzzy null is off", OperatorBuilder.term().path("title").query("godfather").build());
  }

  private static ValidSpec<TermOperator> prefixTrue() {
    return TestSpec.valid(
        "prefix true",
        OperatorBuilder.term().path("title").query("godfather").prefix(true).build());
  }

  private static ValidSpec<TermOperator> regexTrue() {
    return TestSpec.valid(
        "regex true", OperatorBuilder.term().path("title").query("godfather").regex(true).build());
  }

  private static ValidSpec<TermOperator> wildcardTrue() {
    return TestSpec.valid(
        "wildcard true",
        OperatorBuilder.term().path("title").query("godfather").wildcard(true).build());
  }

  private static ValidSpec<TermOperator> fuzzyAndExplicitFalseOptionsNotMutuallyExclusive() {
    return TestSpec.valid(
        "fuzzy and explicit false options not mutually exclusive",
        OperatorBuilder.term()
            .path("title")
            .query("godfather")
            .fuzzy(TermOperatorBuilder.fuzzyBuilder().build())
            .build());
  }
}
