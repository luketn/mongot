package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.shapes.ShapeBuilder;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GeoWithinOperatorTest {

  private static final String SUITE_NAME = "geoWithin";
  private static final BsonDeserializationTestSuite<GeoWithinOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, GeoWithinOperator::fromBson);

  private final TestSpecWrapper<GeoWithinOperator> testSpec;

  public GeoWithinOperatorTest(TestSpecWrapper<GeoWithinOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<GeoWithinOperator>> data() {
    return TEST_SUITE.withExamples(circle(), circleIntegralCenterPoint(), box(), polygon());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<GeoWithinOperator> circle() {
    return TestSpec.valid(
        "circle",
        OperatorBuilder.geoWithin().path("location").shape(ShapeBuilder.circle(1, 2, 3)).build());
  }

  private static ValidSpec<GeoWithinOperator> circleIntegralCenterPoint() {
    return TestSpec.valid(
        "circle, integral center",
        OperatorBuilder.geoWithin().path("location").shape(ShapeBuilder.circle(1, 2, 3)).build());
  }

  private static ValidSpec<GeoWithinOperator> box() {
    return TestSpec.valid(
        "box",
        OperatorBuilder.geoWithin().path("location").shape(ShapeBuilder.box(1, 2, 3, 4)).build());
  }

  private static ValidSpec<GeoWithinOperator> polygon() {
    return TestSpec.valid(
        "polygon",
        OperatorBuilder.geoWithin()
            .path("location")
            .shape(ShapeBuilder.geometry(MockGeometries.POLYGON))
            .build());
  }
}
