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
public class GeoShapeOperatorTest {

  private static final String SUITE_NAME = "geoShape";
  private static final BsonDeserializationTestSuite<GeoShapeOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, GeoShapeOperator::fromBson);

  private final TestSpecWrapper<GeoShapeOperator> testSpec;

  public GeoShapeOperatorTest(TestSpecWrapper<GeoShapeOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<GeoShapeOperator>> data() {
    return TEST_SUITE.withExamples(
        containsPoint(),
        containsPointIntegralPositions(),
        withinPolygon(),
        disjointLine(),
        lineInCollection());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<GeoShapeOperator> containsPoint() {
    return TestSpec.valid(
        "contains point",
        OperatorBuilder.geoShape()
            .path("location")
            .geometry(ShapeBuilder.geometry(MockGeometries.POINT_A))
            .relation(GeoShapeOperator.Relation.CONTAINS)
            .build());
  }

  private static ValidSpec<GeoShapeOperator> containsPointIntegralPositions() {
    return TestSpec.valid(
        "contains point integral positions",
        OperatorBuilder.geoShape()
            .path("location")
            .geometry(ShapeBuilder.geometry(MockGeometries.POINT_A))
            .relation(GeoShapeOperator.Relation.CONTAINS)
            .build());
  }

  private static ValidSpec<GeoShapeOperator> withinPolygon() {
    return TestSpec.valid(
        "within polygon",
        OperatorBuilder.geoShape()
            .path("location")
            .geometry(ShapeBuilder.geometry(MockGeometries.POLYGON))
            .relation(GeoShapeOperator.Relation.WITHIN)
            .build());
  }

  private static ValidSpec<GeoShapeOperator> disjointLine() {
    return TestSpec.valid(
        "disjoint line",
        OperatorBuilder.geoShape()
            .path("location")
            .geometry(ShapeBuilder.geometry(MockGeometries.LINE_AB))
            .relation(GeoShapeOperator.Relation.DISJOINT)
            .build());
  }

  private static ValidSpec<GeoShapeOperator> lineInCollection() {
    return TestSpec.valid(
        "intersects line in geo collection",
        OperatorBuilder.geoShape()
            .path("location")
            .geometry(ShapeBuilder.geometry(MockGeometries.collection(MockGeometries.LINE_AB)))
            .relation(GeoShapeOperator.Relation.INTERSECTS)
            .build());
  }
}
