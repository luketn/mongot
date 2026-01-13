package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.SpanOperatorBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SpanOperatorTest {

  private static final String SUITE_NAME = "span";
  private static final BsonDeserializationTestSuite<SpanOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, SpanOperator::fromBson);

  private final TestSpecWrapper<SpanOperator> testSpec;

  public SpanOperatorTest(TestSpecWrapper<SpanOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<SpanOperator>> data() {
    return TEST_SUITE.withExamples(contains(), first(), near(), or(), term());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<SpanOperator> contains() {
    return TestSpec.valid(
        "contains",
        SpanOperatorBuilder.contains()
            .big(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("big").build())
                    .build())
            .little(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("little").build())
                    .build())
            .spanToReturn(SpanContainsOperator.SpanToReturn.INNER)
            .build());
  }

  private static ValidSpec<SpanOperator> first() {
    return TestSpec.valid(
        "first",
        SpanOperatorBuilder.first()
            .operator(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanOperator> near() {
    return TestSpec.valid(
        "near",
        SpanOperatorBuilder.near()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanOperator> or() {
    return TestSpec.valid(
        "or",
        SpanOperatorBuilder.or()
            .clause(
                SpanOperatorBuilder.term()
                    .term(OperatorBuilder.term().path("title").query("godfather").build())
                    .build())
            .build());
  }

  private static ValidSpec<SpanOperator> term() {
    return TestSpec.valid(
        "term",
        SpanOperatorBuilder.term()
            .term(OperatorBuilder.term().path("title").query("godfather").build())
            .build());
  }
}
