package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.SpanOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SpanTermOperatorTest {

  private static final String SUITE_NAME = "span-term";
  private static final BsonDeserializationTestSuite<SpanTermOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SpanTermOperator::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<SpanTermOperator> testSpec;

  public SpanTermOperatorTest(
      BsonDeserializationTestSuite.TestSpecWrapper<SpanTermOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<SpanTermOperator>> data() {
    return TEST_SUITE.withExamples(simple(), multiPath(), withBoostScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<SpanTermOperator> simple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "simple",
        SpanOperatorBuilder.term()
            .term(OperatorBuilder.term().path("title").query("godfather").build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<SpanTermOperator> multiPath() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "multi path",
        SpanOperatorBuilder.term()
            .term(
                OperatorBuilder.term()
                    .path(StringPathBuilder.withMulti("title", "my-multi"))
                    .query("godfather")
                    .build())
            .build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<SpanTermOperator> withBoostScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "with boost score",
        SpanOperatorBuilder.term()
            .score(ScoreBuilder.valueBoost().value(2).build())
            .term(
                OperatorBuilder.term()
                    .path("title")
                    .query("godfather")
                    .score(ScoreBuilder.valueBoost().value(2).build())
                    .build())
            .build());
  }
}
