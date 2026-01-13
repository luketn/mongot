package com.xgen.mongot.util.geo;

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class GeometryFlattenerTest {

  @Test
  public void testPointsAreVisited() {
    assertFound(MockGeometries.POINT_A, MockGeometries.POINT_A);

    assertFound(MockGeometries.COLLECTION_POLYGON_LINE_POINT, MockGeometries.POINT_A);
    assertFound(
        MockGeometries.collection(MockGeometries.POINT_A, MockGeometries.POINT_B),
        MockGeometries.POINT_B);

    assertFound(
        MockGeometries.multiPoint(MockGeometries.POINT_A, MockGeometries.POINT_B),
        MockGeometries.POINT_A);
    assertFound(
        MockGeometries.multiPoint(MockGeometries.POINT_A, MockGeometries.POINT_B),
        MockGeometries.POINT_B);
  }

  @Test
  public void testLinesAreVisited() {
    assertFound(MockGeometries.LINE_AB, MockGeometries.LINE_AB);

    assertFound(MockGeometries.COLLECTION_POLYGON_LINE_POINT, MockGeometries.LINE_AB);

    assertFound(
        new MultiLineString(MockGeometries.list(MockGeometries.LINE_AB.getCoordinates())),
        MockGeometries.LINE_AB);
  }

  @Test
  public void testPolygonsAreVisited() {
    assertFound(MockGeometries.POLYGON, MockGeometries.POLYGON);

    assertFound(MockGeometries.COLLECTION_POLYGON_LINE_POINT, MockGeometries.POLYGON);

    assertFound(
        new MultiPolygon(MockGeometries.list(MockGeometries.POLYGON.getCoordinates())),
        MockGeometries.POLYGON);
  }

  private void assertFound(Geometry inputGeometry, Geometry shouldBeVisited) {
    Stream<LeafGeometry> flatten = GeometryFlattener.flatten(inputGeometry);
    List<Geometry> unwrapped = flatten.map(this::unwrap).collect(Collectors.toList());
    Assert.assertTrue(unwrapped.contains(shouldBeVisited));
  }

  private Geometry unwrap(LeafGeometry leafGeometry) {
    return switch (leafGeometry.getType()) {
      case LINE_STRING -> leafGeometry.asLineString();
      case POINT -> leafGeometry.asPoint();
      case POLYGON -> leafGeometry.asPolygon();
      default -> throw new IllegalStateException("Unexpected value: " + leafGeometry.getType());
    };
  }
}
