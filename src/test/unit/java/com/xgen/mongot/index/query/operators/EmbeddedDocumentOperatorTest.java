package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.path.string.UnresolvedStringMultiFieldPath;
import com.xgen.mongot.index.query.scores.EmbeddedScore.Aggregate;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.MultiplyExpressionBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.PathExpressionBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.ScoreExpressionBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EmbeddedDocumentOperatorTest {
  private static final String SUITE_NAME = "embeddedDocument";
  private static final BsonDeserializationTestSuite<EmbeddedDocumentOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, EmbeddedDocumentOperator::fromBson);

  private final TestSpecWrapper<EmbeddedDocumentOperator> testSpec;

  public EmbeddedDocumentOperatorTest(TestSpecWrapper<EmbeddedDocumentOperator> testSpec) {
    this.testSpec = testSpec;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<EmbeddedDocumentOperator>> data() {
    return TEST_SUITE.withExamples(
        simple(),
        nestedEmbeddedDocumentOperators(),
        withEmbeddedScore(),
        wildcardPathInOperator(),
        multiPathInOperator(),
        withConstantScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<EmbeddedDocumentOperator> simple() {
    return TestSpec.valid(
        "simple",
        OperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("teachers.first").query("John").build())
                    .must(OperatorBuilder.text().path("teachers.last").query("Smith").build())
                    .build())
            .build());
  }

  private static ValidSpec<EmbeddedDocumentOperator> nestedEmbeddedDocumentOperators() {
    return TestSpec.valid(
        "nested embeddedDocument operators",
        OperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("teachers.first").query("John").build())
                    .must(
                        OperatorBuilder.embeddedDocument()
                            .path("teachers.classes")
                            .operator(
                                OperatorBuilder.compound()
                                    .must(
                                        OperatorBuilder.text()
                                            .path("teachers.classes.name")
                                            .query("art")
                                            .build())
                                    .must(
                                        OperatorBuilder.text()
                                            .path("teachers.classes.grade")
                                            .query("11th")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());
  }

  private static ValidSpec<EmbeddedDocumentOperator> withEmbeddedScore() {
    return TestSpec.valid(
        "with embedded score",
        OperatorBuilder.embeddedDocument()
            .path("teachers.classes")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("teachers.classes.name").query("art").build())
                    .must(
                        OperatorBuilder.text().path("teachers.classes.grade").query("11th").build())
                    .build())
            .score(
                ScoreBuilder.embedded()
                    .aggregate(Aggregate.MAXIMUM)
                    .outerScore(
                        ScoreBuilder.function()
                            .expression(
                                MultiplyExpressionBuilder.builder()
                                    .arg(
                                        PathExpressionBuilder.builder()
                                            .value("overallSchoolRating")
                                            .build())
                                    .arg(ScoreExpressionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build());
  }

  private static ValidSpec<EmbeddedDocumentOperator> wildcardPathInOperator() {
    return TestSpec.valid(
        "wildcard path in operator",
        OperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("teachers.first.*").query("John").build())
                    .must(OperatorBuilder.text().path("teachers.last.*").query("Smith").build())
                    .build())
            .build());
  }

  private static ValidSpec<EmbeddedDocumentOperator> multiPathInOperator() {
    return TestSpec.valid(
        "multi path in operator",
        OperatorBuilder.embeddedDocument()
            .path("teachers")
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.text()
                            .path(
                                new UnresolvedStringMultiFieldPath(
                                    FieldPath.parse("teachers.first"), "my-multi"))
                            .query("John")
                            .build())
                    .must(OperatorBuilder.text().path("teachers.last.*").query("Smith").build())
                    .build())
            .build());
  }

  private static ValidSpec<EmbeddedDocumentOperator> withConstantScore() {
    return TestSpec.valid(
        "with constant score",
        OperatorBuilder.embeddedDocument()
            .path("teachers.classes")
            .operator(
                OperatorBuilder.compound()
                    .must(OperatorBuilder.text().path("teachers.classes.name").query("art").build())
                    .must(
                        OperatorBuilder.text().path("teachers.classes.grade").query("11th").build())
                    .build())
            .score(ScoreBuilder.constant().value(1).build())
            .build());
  }
}
