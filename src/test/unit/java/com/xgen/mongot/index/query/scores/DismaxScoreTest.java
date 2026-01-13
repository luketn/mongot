package com.xgen.mongot.index.query.scores;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DismaxScoreTest {

  private static final String SUITE_NAME = "dismax-score";
  private static final BsonDeserializationTestSuite<DismaxScore> TEST_SUITE =
      fromDocument(ScoreTests.RESOURCES_PATH, SUITE_NAME, DismaxScore::fromBson);

  private final BsonDeserializationTestSuite.TestSpecWrapper<DismaxScore> testSpec;

  public DismaxScoreTest(BsonDeserializationTestSuite.TestSpecWrapper<DismaxScore> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<DismaxScore>> data() {
    return TEST_SUITE.withExamples(
        zeroTieBreakerScore(),
        oneTieBreakerScore(),
        betweenZeroAndOneTieBreakerScore(),
        defaultTieBreakerScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static BsonDeserializationTestSuite.ValidSpec<DismaxScore> zeroTieBreakerScore() {
    return TestSpec.valid(
        "zero tieBreakerScore", ScoreBuilder.dismax().tieBreakerScore(0f).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<DismaxScore> oneTieBreakerScore() {
    return TestSpec.valid("one tieBreakerScore", ScoreBuilder.dismax().tieBreakerScore(1f).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<DismaxScore>
      betweenZeroAndOneTieBreakerScore() {
    return TestSpec.valid(
        "between zero and one tieBreakerScore",
        ScoreBuilder.dismax().tieBreakerScore(0.5f).build());
  }

  private static BsonDeserializationTestSuite.ValidSpec<DismaxScore> defaultTieBreakerScore() {
    return TestSpec.valid(
        "default tieBreakerScore", ScoreBuilder.dismax().tieBreakerScore(1f).build());
  }
}
