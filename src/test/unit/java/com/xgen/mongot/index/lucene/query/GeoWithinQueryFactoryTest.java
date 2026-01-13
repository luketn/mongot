package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.shapes.ShapeBuilder;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.io.IOException;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BoostQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GeoWithinQueryFactoryTest {
  private LuceneSearchTranslation luceneTranslation;

  /** init with a "geo" field mapped as geo. */
  @Before
  public void initTranslation() {
    DocumentFieldDefinition mapping =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "geo",
                FieldDefinitionBuilder.builder()
                    .geo(GeoFieldDefinitionBuilder.builder().indexShapes(true).build())
                    .build())
            .build();
    this.luceneTranslation = LuceneSearchTranslation.mapped(mapping);
  }

  @Test
  public void testWithinCircle() throws InvalidQueryException, IOException {
    double lon = 10.0;
    double lat = 12.0;

    var operator =
        OperatorBuilder.geoWithin().path("geo").shape(ShapeBuilder.circle(lon, lat, 1042)).build();

    var expected = LatLonPoint.newDistanceQuery(getPointField(), lat, lon, 1042);
    this.luceneTranslation.assertTranslatedTo(operator, expected);
  }

  @Test
  public void testWithinCircleWithBoost() throws InvalidQueryException, IOException {
    double lon = 10.0;
    double lat = 12.0;
    var operator =
        OperatorBuilder.geoWithin()
            .score(ScoreBuilder.valueBoost().value(3).build())
            .path("geo")
            .shape(ShapeBuilder.circle(lon, lat, 1000))
            .build();
    var expectedInner = LatLonPoint.newDistanceQuery(getPointField(), lat, lon, 1000);
    var expected = new BoostQuery(expectedInner, 3);
    this.luceneTranslation.assertTranslatedTo(operator, expected);
  }

  @Test
  public void testWithinCircleOutOfBoundsThrows() {
    double lon = 10.0;
    double lat = 500;

    var operator =
        OperatorBuilder.geoWithin().path("geo").shape(ShapeBuilder.circle(lon, lat, 1000)).build();

    Assert.assertThrows(
        InvalidQueryException.class, () -> this.luceneTranslation.translate(operator));
  }

  @Test
  public void testWithinBox() throws InvalidQueryException, IOException {
    double bottomLon = -122.522848;
    double topLon = -122.345366;
    double bottomLat = 37.696776;
    double topLat = 37.809754;
    var operator =
        OperatorBuilder.geoWithin()
            .path("geo")
            .shape(ShapeBuilder.box(bottomLon, bottomLat, topLon, topLat))
            .build();

    var expected = LatLonPoint.newBoxQuery(getPointField(), bottomLat, topLat, bottomLon, topLon);
    this.luceneTranslation.assertTranslatedTo(operator, expected);
  }

  @Test
  public void testWithinPolygon() throws InvalidQueryException, IOException {
    var operator =
        OperatorBuilder.geoWithin()
            .path("geo")
            .shape(ShapeBuilder.geometry(MockGeometries.POLYGON))
            .build();

    var lucenePolygon = new Polygon(MockGeometries.POLYGON_LATS, MockGeometries.POLYGON_LONS);
    var expected = LatLonPoint.newPolygonQuery(getPointField(), lucenePolygon);
    this.luceneTranslation.assertTranslatedTo(operator, expected);
  }

  @Test
  public void testWithinMultiPolygon() throws InvalidQueryException, IOException {
    var operator =
        OperatorBuilder.geoWithin()
            .path("geo")
            .shape(ShapeBuilder.geometry(MockGeometries.MULTI_POLYGON))
            .build();

    var lucenePolygon = new Polygon(MockGeometries.POLYGON_LATS, MockGeometries.POLYGON_LONS);
    var expected = LatLonPoint.newPolygonQuery(getPointField(), lucenePolygon);
    this.luceneTranslation.assertTranslatedTo(operator, expected);
  }

  @Test
  public void testQueryFieldNotMappedToGeoInvalid() {
    var operator =
        OperatorBuilder.geoWithin().path("not_geo").shape(ShapeBuilder.circle(1, 2, 3)).build();
    Assert.assertThrows(
        InvalidQueryException.class, () -> LuceneSearchTranslation.get().translate(operator));
  }

  @Test
  public void testQueryFieldWithoutIndexShapesIsOkay() throws Exception {
    var operator =
        OperatorBuilder.geoWithin()
            .path("geo_not_shape")
            .shape(ShapeBuilder.circle(1, 2, 3))
            .build();

    DocumentFieldDefinition mapping =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "geo_not_shape",
                FieldDefinitionBuilder.builder()
                    .geo(GeoFieldDefinitionBuilder.builder().indexShapes(false).build())
                    .build())
            .build();
    // valid
    LuceneSearchTranslation.mapped(mapping).translate(operator);
  }

  private String getPointField() {
    return "$type:geoPoint/geo";
  }
}
