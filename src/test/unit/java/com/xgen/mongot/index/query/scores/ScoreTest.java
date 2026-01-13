package com.xgen.mongot.index.query.scores;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ScoreTest {

  private static final String SUITE_NAME = "score";
  private static final BsonDeserializationTestSuite<Score> TEST_SUITE =
      fromDocument(ScoreTests.RESOURCES_PATH, SUITE_NAME, ScoreTest::fromBsonAllowAll);

  private final TestSpecWrapper<Score> testSpec;

  public ScoreTest(TestSpecWrapper<Score> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<Score>> data() {
    return TEST_SUITE.withExamples(
        valueBoost(), pathBoost(), constant(), dismax(), function(), embedded());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<Score> valueBoost() {
    return TestSpec.valid("value boost", ScoreTest::assertCorrectValueBoost);
  }

  private static ValidSpec<Score> pathBoost() {
    return TestSpec.valid("path boost", ScoreTest::assertCorrectPathBoost);
  }

  private static ValidSpec<Score> constant() {
    return TestSpec.valid(
        "constant", score -> Assert.assertEquals(Score.Type.CONSTANT, score.getType()));
  }

  private static ValidSpec<Score> dismax() {
    return TestSpec.valid(
        "dismax", score -> Assert.assertEquals(Score.Type.DISMAX, score.getType()));
  }

  private static ValidSpec<Score> function() {
    return TestSpec.valid(
        "function", score -> Assert.assertEquals(Score.Type.FUNCTION, score.getType()));
  }

  private static ValidSpec<Score> embedded() {
    return TestSpec.valid(
        "embedded", score -> Assert.assertEquals(Score.Type.EMBEDDED, score.getType()));
  }

  private static void assertCorrectValueBoost(Score score) {
    Assert.assertEquals(Score.Type.VALUE_BOOST, score.getType());
    Assert.assertEquals(score, ScoreBuilder.valueBoost().value(1).build());
  }

  private static void assertCorrectPathBoost(Score score) {
    Assert.assertEquals(Score.Type.PATH_BOOST, score.getType());
    Assert.assertEquals(score, ScoreBuilder.pathBoost().path("popularity").undefined(3456).build());
  }

  private static Score fromBsonAllowAll(DocumentParser parser) throws BsonParseException {
    var boost = parser.getField(Score.Fields.BOOST);
    var constant = parser.getField(Score.Fields.CONSTANT);
    var function = parser.getField(Score.Fields.FUNCTION);
    var dismax = parser.getField(Score.Fields.DISMAX);
    var embedded = parser.getField(Score.Fields.EMBEDDED);

    return parser.getGroup().exactlyOneOf(boost, constant, function, dismax, embedded);
  }
}
