package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class QueryStringOperatorTest {

  private static final String SUITE_NAME = "query-string";
  private static final BsonDeserializationTestSuite<QueryStringOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, QueryStringOperator::fromBson);

  private final TestSpecWrapper<QueryStringOperator> testSpec;

  public QueryStringOperatorTest(TestSpecWrapper<QueryStringOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<QueryStringOperator>> data() {
    return TEST_SUITE.withExamples(simple(), emptyDefaultPath(), withBoostScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<QueryStringOperator> simple() {
    return TestSpec.valid(
        "simple", OperatorBuilder.queryString().defaultPath("title").query("foo AND bar").build());
  }

  private static ValidSpec<QueryStringOperator> emptyDefaultPath() {
    return TestSpec.valid(
        "empty default path",
        OperatorBuilder.queryString().defaultPath("").query("foo AND bar").build());
  }

  private static ValidSpec<QueryStringOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.queryString()
            .defaultPath("title")
            .query("foo AND bar")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }
}
