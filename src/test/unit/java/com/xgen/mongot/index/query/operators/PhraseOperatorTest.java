package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PhraseOperatorTest {

  private static final String SUITE_NAME = "phrase";
  private static final BsonDeserializationTestSuite<PhraseOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, PhraseOperator::fromBson);

  private final TestSpecWrapper<PhraseOperator> testSpec;

  public PhraseOperatorTest(TestSpecWrapper<PhraseOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<PhraseOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(),
        multiPath(),
        withBoostScore(),
        withSynonymsAndBoostScore(),
        withSynonyms(),
        withSynonymsAndSlop(),
        zeroSlop(),
        positiveSlop());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<PhraseOperator> simple() {
    return TestSpec.valid(
        "simple", OperatorBuilder.phrase().path("title").query("godfather").build());
  }

  private static ValidSpec<PhraseOperator> multiPath() {
    return TestSpec.valid(
        "multi path",
        OperatorBuilder.phrase()
            .path(UnresolvedStringPathBuilder.withMulti("title", "my-multi"))
            .query("godfather")
            .build());
  }

  private static ValidSpec<PhraseOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.phrase()
            .path("title")
            .query("godfather")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<PhraseOperator> withSynonymsAndBoostScore() {
    return TestSpec.valid(
        "with synonyms and boost score",
        OperatorBuilder.phrase()
            .path("title")
            .query("godfather")
            .synonyms("collection")
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }

  private static ValidSpec<PhraseOperator> withSynonyms() {
    return TestSpec.valid(
        "with synonyms",
        OperatorBuilder.phrase().path("title").query("godfather").synonyms("collection").build());
  }

  private static ValidSpec<PhraseOperator> zeroSlop() {
    return TestSpec.valid(
        "zero slop", OperatorBuilder.phrase().path("title").query("godfather").slop(0).build());
  }

  private static ValidSpec<PhraseOperator> positiveSlop() {
    return TestSpec.valid(
        "positive slop",
        OperatorBuilder.phrase().path("title").query("godfather").slop(13).build());
  }

  private static ValidSpec<PhraseOperator> withSynonymsAndSlop() {
    return TestSpec.valid(
        "with synonyms and slop",
        OperatorBuilder.phrase()
            .path("title")
            .query("godfather")
            .synonyms("collection")
            .slop(13)
            .build());
  }
}
