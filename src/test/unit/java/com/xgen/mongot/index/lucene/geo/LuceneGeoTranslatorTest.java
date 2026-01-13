package com.xgen.mongot.index.lucene.geo;

import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import com.xgen.mongot.util.geo.LeafGeometry;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class LuceneGeoTranslatorTest {

  private static final Position A = new Position(10.0, 10.0);
  private static final Position B = new Position(11.0, 11.0);
  private static final Position C = new Position(10.0, 12.0);
  // Must use ArrayList here: JAVA-3635
  private static final List<Position> ABC = new ArrayList<>(List.of(A, B, C, A));
  private static final double[] ABC_LATS = {10.0, 11.0, 12.0, 10.0};
  private static final double[] ABC_LONS = {10.0, 11.0, 10.0, 10.0};

  @Test
  public void testPolygonTranslated() throws InvalidGeoPosition {
    Polygon polygon = MockGeometries.POLYGON;
    var actual = LuceneGeoTranslator.polygon(polygon);

    var expected =
        new org.apache.lucene.geo.Polygon(MockGeometries.POLYGON_LATS, MockGeometries.POLYGON_LONS);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultiPolygon() throws InvalidGeoPosition {
    Polygon polygon = new Polygon(ABC);
    var multi = new MultiPolygon(new ArrayList<>(List.of(polygon.getCoordinates())));
    var actual = LuceneGeoTranslator.multiPolygon(multi);
    Assert.assertEquals(List.of(LuceneGeoTranslator.polygon(polygon)), actual);
  }

  @Test
  public void testPolygonWithHolesTranslated() throws InvalidGeoPosition {
    Polygon polygon =
        new Polygon(
            MockGeometries.POLYGON.getCoordinates().getExterior(),
            MockGeometries.INNER_POLYGON.getCoordinates().getExterior());
    var actual = LuceneGeoTranslator.polygon(polygon);

    var expectedHole =
        new org.apache.lucene.geo.Polygon(
            MockGeometries.INNER_POLYGON_LATS, MockGeometries.INNER_POLYGON_LONS);
    var expected =
        new org.apache.lucene.geo.Polygon(
            MockGeometries.POLYGON_LATS, MockGeometries.POLYGON_LONS, expectedHole);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLineTranslated() throws InvalidGeoPosition {
    LineString line = new LineString(ABC);
    org.apache.lucene.geo.Line actual = LuceneGeoTranslator.line(line);

    var expected = new org.apache.lucene.geo.Line(ABC_LATS, ABC_LONS);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPointLonLat() throws InvalidGeoPosition {
    var point = new Point(new Position(1.0, 2.0));
    var translated = LuceneGeoTranslator.point(point);
    var lon = translated.getLon();
    Assert.assertEquals("longitude is first", 1.0, lon, 0.00001);
    var lat = translated.getLat();
    Assert.assertEquals("latitude is second", 2.0, lat, 0.00001);
  }

  @Test
  public void testPointCoordinatesInBounds() throws InvalidGeoPosition {
    var invalidLon = new Point(new Position(1000.0, 2.0));
    Assert.assertThrows(InvalidGeoPosition.class, () -> LuceneGeoTranslator.point(invalidLon));
    // Bounds are inclusive
    LuceneGeoTranslator.point(new Point(new Position(180, 2.0)));
    LuceneGeoTranslator.point(new Point(new Position(-180, 2.0)));

    var invalidLat = new Point(new Position(0.0, 90.01));
    Assert.assertThrows(InvalidGeoPosition.class, () -> LuceneGeoTranslator.point(invalidLat));
    // Bounds are inclusive
    LuceneGeoTranslator.point(new Point(new Position(180, 90)));
    LuceneGeoTranslator.point(new Point(new Position(-180, -90)));
  }

  @Test
  public void testGeometryLeafTranslationPolygon() throws InvalidGeoPosition {
    var polygon = MockGeometries.POLYGON;
    var expected = LuceneGeoTranslator.polygon(polygon);

    var actual = LuceneGeoTranslator.translate(new LeafGeometry(polygon));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGeometryLeafTranslationLine() throws InvalidGeoPosition {
    var line = MockGeometries.LINE_AB;
    var expected = LuceneGeoTranslator.line(line);

    var actual = LuceneGeoTranslator.translate(new LeafGeometry(line));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGeometryLeafTranslationPoint() throws InvalidGeoPosition {
    var point = MockGeometries.POINT_A;
    var expected = new org.apache.lucene.geo.Point(12, 10);

    var actual = LuceneGeoTranslator.translate(new LeafGeometry(point));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testShapeCoordinatesOutOfBounds() {
    Assert.assertThrows(
        InvalidGeoPosition.class,
        () -> LuceneGeoTranslator.line(MockGeometries.OUT_OF_BOUNDS_LINE));
  }
}
