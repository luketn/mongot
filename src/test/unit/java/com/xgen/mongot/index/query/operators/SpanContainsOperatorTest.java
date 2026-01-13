package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.operators.SpanContainsOperator.SpanToReturn;
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
public class SpanContainsOperatorTest {

  private static final String SUITE_NAME = "span-contains";
  private static final BsonDeserializationTestSuite<SpanContainsOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SpanContainsOperator::fromBson);

  private final TestSpecWrapper<SpanContainsOperator> testSpec;

  public SpanContainsOperatorTest(TestSpecWrapper<SpanContainsOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SpanContainsOperator>> data() {
    return TEST_SUITE.withExamples(inner(), outer(), withBoostScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SpanContainsOperator> inner() {
    return TestSpec.valid(
        "inner",
        SpanOperatorBuilder.contains()
            .big(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("big").build())
                    .build())
            .little(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("little").build())
                    .build())
            .spanToReturn(SpanToReturn.INNER)
            .build());
  }

  private static ValidSpec<SpanContainsOperator> outer() {
    return TestSpec.valid(
        "outer",
        SpanOperatorBuilder.contains()
            .big(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("big").build())
                    .build())
            .little(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("little").build())
                    .build())
            .spanToReturn(SpanToReturn.OUTER)
            .build());
  }

  private static ValidSpec<SpanContainsOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        SpanOperatorBuilder.contains()
            .big(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("big").build())
                    .build())
            .little(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("little").build())
                    .build())
            .spanToReturn(SpanToReturn.INNER)
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }
}
