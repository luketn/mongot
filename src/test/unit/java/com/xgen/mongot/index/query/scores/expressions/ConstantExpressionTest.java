package com.xgen.mongot.index.query.scores.expressions;

import static com.xgen.testing.BsonDeserializationTestSuite.fromValue;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.query.scores.expressions.ConstantExpressionBuilder;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ConstantExpressionTest.TestDeserialization.class,
      ConstantExpressionTest.TestSerialization.class,
    })
public class ConstantExpressionTest {
  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "constant-expression-deserialization";
    private static final String RESOURCES_PATH =
        "src/test/unit/resources/index/query/scores/expressions/";
    private static final BsonDeserializationTestSuite<ConstantExpression> TEST_SUITE =
        fromValue(RESOURCES_PATH, SUITE_NAME, ConstantExpression::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ConstantExpression> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<ConstantExpression> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ConstantExpression>>
        data() {
      return TEST_SUITE.withExamples(
          negativeConstant(), zeroValuedConstant(), positiveConstant(), integerConstant());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConstantExpression> negativeConstant() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "negative constant", ConstantExpressionBuilder.builder().constant(-4328.54).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConstantExpression> zeroValuedConstant() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "zero-valued constant", ConstantExpressionBuilder.builder().constant(0.0).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConstantExpression> positiveConstant() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "positive constant", ConstantExpressionBuilder.builder().constant(2039.38).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConstantExpression> integerConstant() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "integer constant", ConstantExpressionBuilder.builder().constant(250).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    public static final String SUITE_NAME = "constant-expression-serialization";
    private static final String RESOURCES_PATH =
        "src/test/unit/resources/index/query/scores/expressions/";
    public static final BsonSerializationTestSuite<ConstantExpression> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<ConstantExpression> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<ConstantExpression> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ConstantExpression>> data() {
      return Arrays.asList(negativeConstant(), zeroValuedConstant(), positiveConstant());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ConstantExpression> negativeConstant() {
      return BsonSerializationTestSuite.TestSpec.create(
          "negative constant", ConstantExpressionBuilder.builder().constant(-3378.99).build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConstantExpression> zeroValuedConstant() {
      return BsonSerializationTestSuite.TestSpec.create(
          "zero-valued constant", ConstantExpressionBuilder.builder().constant(0.0).build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConstantExpression> positiveConstant() {
      return BsonSerializationTestSuite.TestSpec.create(
          "positive constant", ConstantExpressionBuilder.builder().constant(4567.32).build());
    }
  }
}
