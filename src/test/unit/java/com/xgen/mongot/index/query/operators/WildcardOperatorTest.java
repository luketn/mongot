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
public class WildcardOperatorTest {

  private static final String SUITE_NAME = "wildcard";
  private static final BsonDeserializationTestSuite<WildcardOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, WildcardOperator::fromBson);

  private final TestSpecWrapper<WildcardOperator> testSpec;

  public WildcardOperatorTest(TestSpecWrapper<WildcardOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<WildcardOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(), multiPath(), withBoostScore(), allowAnalyzedFieldTrue());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<WildcardOperator> simple() {
    return TestSpec.valid(
        "simple", OperatorBuilder.wildcard().path("title").query("godfather").build());
  }

  private static ValidSpec<WildcardOperator> multiPath() {
    return TestSpec.valid(
        "multi path",
        OperatorBuilder.wildcard()
            .path(UnresolvedStringPathBuilder.withMulti("title", "my-multi"))
            .query("godfather")
            .build());
  }

  private static ValidSpec<WildcardOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.wildcard()
            .path("title")
            .query("godfather")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<WildcardOperator> allowAnalyzedFieldTrue() {
    return TestSpec.valid(
        "allowAnalyzedField true",
        OperatorBuilder.wildcard()
            .path("title")
            .query("godfather")
            .allowAnalyzedField(true)
            .build());
  }
}
