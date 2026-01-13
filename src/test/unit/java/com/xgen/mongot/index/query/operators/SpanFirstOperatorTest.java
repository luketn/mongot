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
public class SpanFirstOperatorTest {

  private static final String SUITE_NAME = "span-first";
  private static final BsonDeserializationTestSuite<SpanFirstOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SpanFirstOperator::fromBson);

  private final TestSpecWrapper<SpanFirstOperator> testSpec;

  public SpanFirstOperatorTest(TestSpecWrapper<SpanFirstOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SpanFirstOperator>> data() {
    return TEST_SUITE.withExamples(simple(), withBoostScore(), explicitEndPositionLte());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SpanFirstOperator> simple() {
    return TestSpec.valid(
        "simple",
        SpanOperatorBuilder.first()
            .operator(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanFirstOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        SpanOperatorBuilder.first()
            .score(ScoreBuilder.valueBoost().value(2).build())
            .operator(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanFirstOperator> explicitEndPositionLte() {
    return TestSpec.valid(
        "explicit endPositionLte",
        SpanOperatorBuilder.first()
            .operator(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .endPositionLte(13)
            .build());
  }
}
