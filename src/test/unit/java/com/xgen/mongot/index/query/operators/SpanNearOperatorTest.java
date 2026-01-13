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
public class SpanNearOperatorTest {

  private static final String SUITE_NAME = "span-near";
  private static final BsonDeserializationTestSuite<SpanNearOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SpanNearOperator::fromBson);

  private final TestSpecWrapper<SpanNearOperator> testSpec;

  public SpanNearOperatorTest(TestSpecWrapper<SpanNearOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SpanNearOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(), withBoostScore(), multipleClauses(), inOrderTrue(), positiveSlop());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SpanNearOperator> simple() {
    return TestSpec.valid(
        "simple",
        SpanOperatorBuilder.near()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanNearOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        SpanOperatorBuilder.near()
            .score(ScoreBuilder.valueBoost().value(2).build())
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanNearOperator> multipleClauses() {
    return TestSpec.valid(
        "multiple clauses",
        SpanOperatorBuilder.near()
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

  private static ValidSpec<SpanNearOperator> inOrderTrue() {
    return TestSpec.valid(
        "inOrder true",
        SpanOperatorBuilder.near()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .inOrder(true)
            .build());
  }

  private static ValidSpec<SpanNearOperator> positiveSlop() {
    return TestSpec.valid(
        "positive slop",
        SpanOperatorBuilder.near()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .slop(13)
            .build());
  }
}
