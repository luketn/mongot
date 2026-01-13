package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ExistsOperatorTest {

  private static final String SUITE_NAME = "exists";
  private static final BsonDeserializationTestSuite<ExistsOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, ExistsOperator::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<ExistsOperator> testSpec;

  public ExistsOperatorTest(BsonDeserializationTestSuite.TestSpecWrapper<ExistsOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ExistsOperator>> data() {
    return TEST_SUITE.withExamples(simple(), emptyPath(), withScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<ExistsOperator> simple() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "simple", OperatorBuilder.exists().path("title").build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<ExistsOperator> emptyPath() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "empty path", OperatorBuilder.exists().path("").build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<ExistsOperator> withScore() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "with score",
        OperatorBuilder.exists()
            .score(ScoreBuilder.valueBoost().value(2).build())
            .path("title")
            .build());
  }
}
