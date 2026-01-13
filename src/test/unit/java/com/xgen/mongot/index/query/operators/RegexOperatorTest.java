package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RegexOperatorTest {

  private static final String SUITE_NAME = "regex";
  private static final BsonDeserializationTestSuite<RegexOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, RegexOperator::fromBson);

  private final TestSpecWrapper<RegexOperator> testSpec;

  public RegexOperatorTest(TestSpecWrapper<RegexOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<RegexOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(), multiPath(), withBoostScore(), allowAnalyzedFieldTrue());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<RegexOperator> simple() {
    return TestSpec.valid(
        "simple", OperatorBuilder.regex().path("title").query("godfather").build());
  }

  private static ValidSpec<RegexOperator> multiPath() {
    return TestSpec.valid(
        "multi path",
        OperatorBuilder.regex()
            .path(UnresolvedStringPathBuilder.withMulti("title", "my-multi"))
            .query("godfather")
            .build());
  }

  private static ValidSpec<RegexOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.regex()
            .path("title")
            .query("godfather")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<RegexOperator> allowAnalyzedFieldTrue() {
    return TestSpec.valid(
        "allowAnalyzedField true",
        OperatorBuilder.regex().path("title").query("godfather").allowAnalyzedField(true).build());
  }
}
