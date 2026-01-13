package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.SpanOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SpanOrOperatorTest {

  private static final String SUITE_NAME = "span-or";
  private static final BsonDeserializationTestSuite<SpanOrOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SpanOrOperator::fromBson);

  private final TestSpecWrapper<SpanOrOperator> testSpec;

  public SpanOrOperatorTest(TestSpecWrapper<SpanOrOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SpanOrOperator>> data() {
    return TEST_SUITE.withExamples(simple(), withBoostScore(), multipleClauses());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SpanOrOperator> simple() {
    return TestSpec.valid(
        "simple",
        SpanOperatorBuilder.or()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanOrOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        SpanOperatorBuilder.or()
            .score(ScoreBuilder.valueBoost().value(2).build())
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanOrOperator> multipleClauses() {
    return TestSpec.valid(
        "multiple clauses",
        SpanOperatorBuilder.or()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godmother").build())
                    .build())
            .build());
  }
}
