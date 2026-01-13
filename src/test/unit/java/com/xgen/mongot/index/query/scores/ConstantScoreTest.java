package com.xgen.mongot.index.query.scores;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConstantScoreTest {

  private static final String SUITE_NAME = "constant-score";
  private static final BsonDeserializationTestSuite<ConstantScore> TEST_SUITE =
      fromDocument(ScoreTests.RESOURCES_PATH, SUITE_NAME, ConstantScore::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<ConstantScore> testSpec;

  public ConstantScoreTest(BsonDeserializationTestSuite.TestSpecWrapper<ConstantScore> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ConstantScore>> data() {
    return TEST_SUITE.withExamples(positiveValue(), zeroValue());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<ConstantScore> positiveValue() {
    return TestSpec.valid("positive value", ScoreBuilder.constant().value(13f).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<ConstantScore> zeroValue() {
    return TestSpec.valid("zero value", ScoreBuilder.constant().value(0f).build());
  }
}
