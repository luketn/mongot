package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.AutocompleteOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AutocompleteOperatorTest {
  private static final String SUITE_NAME = "autocomplete";
  private static final BsonDeserializationTestSuite<AutocompleteOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, AutocompleteOperator::fromBson);

  private final TestSpecWrapper<AutocompleteOperator> testSpec;

  public AutocompleteOperatorTest(TestSpecWrapper<AutocompleteOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<AutocompleteOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(),
        withBoostScore(),
        defaultFuzzy(),
        explicitFuzzyOptions(),
        fuzzyNullIsOff(),
        tokenOrderAny(),
        tokenOrderSequential());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<AutocompleteOperator> simple() {
    return TestSpec.valid(
        "simple",
        OperatorBuilder.autocomplete()
            .path("description")
            .query("steakh")
            .tokenOrder(AutocompleteOperator.TokenOrder.ANY)
            .build());
  }

  private static ValidSpec<AutocompleteOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.autocomplete()
            .path("description")
            .query("steakh")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<AutocompleteOperator> defaultFuzzy() {
    return TestSpec.valid(
        "default fuzzy",
        OperatorBuilder.autocomplete()
            .path("description")
            .query("steakh")
            .fuzzy(AutocompleteOperatorBuilder.fuzzyBuilder().build())
            .build());
  }

  private static ValidSpec<AutocompleteOperator> explicitFuzzyOptions() {
    return TestSpec.valid(
        "explicit fuzzy options",
        OperatorBuilder.autocomplete()
            .path("description")
            .query("steakh")
            .fuzzy(
                AutocompleteOperatorBuilder.fuzzyBuilder()
                    .maxEdits(1)
                    .maxExpansions(2)
                    .prefixLength(3)
                    .build())
            .build());
  }

  private static ValidSpec<AutocompleteOperator> fuzzyNullIsOff() {
    return TestSpec.valid(
        "fuzzy null is off",
        OperatorBuilder.autocomplete().path("description").query("steakh").build());
  }

  private static ValidSpec<AutocompleteOperator> tokenOrderAny() {
    return TestSpec.valid(
        "tokenOrder any",
        OperatorBuilder.autocomplete()
            .path("description")
            .query("steakh")
            .tokenOrder(AutocompleteOperator.TokenOrder.ANY)
            .build());
  }

  private static ValidSpec<AutocompleteOperator> tokenOrderSequential() {
    return TestSpec.valid(
        "tokenOrder sequential",
        OperatorBuilder.autocomplete()
            .path("description")
            .query("steakh")
            .tokenOrder(AutocompleteOperator.TokenOrder.SEQUENTIAL)
            .build());
  }
}
