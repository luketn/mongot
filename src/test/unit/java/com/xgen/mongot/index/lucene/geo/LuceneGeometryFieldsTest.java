package com.xgen.mongot.index.lucene.geo;

import com.google.common.truth.Correspondence;
import com.google.common.truth.Truth;
import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.junit.Assert;
import org.junit.Test;

public class LuceneGeometryFieldsTest {

  private static final String FIELD_NAME = "geofield";

  // This has to do _just_ enough to compare LatLonPoint (which has an 8 byte array ref) and
  // LatLonDocValuesField (which has a long ref).
  private static boolean compareFields(Object a, Object e) {
    if (!(a instanceof Field fa) || !(e instanceof Field fe)) {
      return false;
    }
    // compare field name and type.
    if (!fa.name().equals(fe.name()) || fa.fieldType() != fe.fieldType()) {
      return false;
    }
    // check the numeric value ref.
    if (fa.numericValue() != null && fe.numericValue() != null) {
      return fa.numericValue().equals(fe.numericValue());
    } else if (fa.binaryValue() != null && fe.binaryValue() != null) {
      return fa.binaryValue().equals(fe.binaryValue());
    } else {
      return false;
    }
  }

  @Test
  public void testPointAsGeoPoint() {
    var actual = pointFields(MockGeometries.POINT_A);
    var latLon = new LatLonPoint(FIELD_NAME, 12.0, 10.0);
    var latLonDocs = new LatLonDocValuesField(FIELD_NAME, 12.0, 10.0);
    var expected = List.of(latLon, latLonDocs);
    Truth.assertThat(actual)
        .comparingElementsUsing(
            Correspondence.from(LuceneGeometryFieldsTest::compareFields, "FieldCmp"))
        .containsExactlyElementsIn(expected);
  }

  @Test
  public void testOutOfBoundsPointNotIndexed() {
    var actual = pointFields(MockGeometries.OUT_OF_BOUNDS_POINT);
    var expected = Collections.emptyList();
    Truth.assertThat(actual)
        .comparingElementsUsing(
            Correspondence.from(LuceneGeometryFieldsTest::compareFields, "FieldCmp"))
        .containsExactlyElementsIn(expected);
  }

  @Test
  public void testGeoPoinIndexesPointsFromGeoCollection() {
    var actual = pointFields(MockGeometries.COLLECTION_POLYGON_LINE_POINT);
    Assert.assertEquals("one point accounts to two fields", 2, actual.size());
  }

  @Test
  public void testPointAsGeoShape() {
    Field[] actual = shapeFields(MockGeometries.POINT_A);
    Field[] expected = LatLonShape.createIndexableFields(FIELD_NAME, 12.0, 10.0);
    Truth.assertThat(Arrays.asList(actual))
        .comparingElementsUsing(
            Correspondence.from(LuceneGeometryFieldsTest::compareFields, "FieldCmp"))
        .containsExactlyElementsIn(Arrays.asList(expected));
  }

  @Test
  public void testShapeIsNotIndexedAsPoint() {
    var actual = pointFields(MockGeometries.POLYGON);
    Assert.assertEquals(0, actual.size());
  }

  @Test
  public void testLineAsGeoShape() {
    Field[] actual = shapeFields(MockGeometries.LINE_AB);

    Line line = new Line(MockGeometries.LINE_LATS, MockGeometries.LINE_LONS);
    Field[] expected = LatLonShape.createIndexableFields(FIELD_NAME, line);
    Truth.assertThat(Arrays.asList(actual))
        .comparingElementsUsing(
            Correspondence.from(LuceneGeometryFieldsTest::compareFields, "FieldCmp"))
        .containsExactlyElementsIn(Arrays.asList(expected));
  }

  @Test
  public void testPolygonAsGeoShape() throws InvalidGeoPosition {
    Field[] actual =
        LuceneGeometryFields.forGeoShape(
                MockGeometries.POLYGON, FIELD_NAME, createMockIndexingMetricsUpdater())
            .toArray(ShapeField.Triangle[]::new);

    Polygon polygon = LuceneGeoTranslator.polygon(MockGeometries.POLYGON);

    Field[] expected = LatLonShape.createIndexableFields(FIELD_NAME, polygon);
    Truth.assertThat(Arrays.asList(actual))
        .comparingElementsUsing(
            Correspondence.from(LuceneGeometryFieldsTest::compareFields, "FieldCmp"))
        .containsExactlyElementsIn(Arrays.asList(expected));
  }

  @Test
  public void testGeoCollectionAsGeoShape() throws InvalidGeoPosition {
    Field[] actual = shapeFields(MockGeometries.COLLECTION_POLYGON_LINE_POINT);

    Line line = new Line(MockGeometries.LINE_LATS, MockGeometries.LINE_LONS);
    Polygon polygon = LuceneGeoTranslator.polygon(MockGeometries.POLYGON);

    // expected to collect the fields from all the nested geometries, in order, and flattened
    var expected =
        Stream.of(
                LatLonShape.createIndexableFields(FIELD_NAME, polygon),
                LatLonShape.createIndexableFields(FIELD_NAME, line),
                LatLonShape.createIndexableFields(FIELD_NAME, 12.0, 10.0))
            .flatMap(Arrays::stream)
            .toArray(Field[]::new);

    Truth.assertThat(Arrays.asList(actual))
        .comparingElementsUsing(
            Correspondence.from(LuceneGeometryFieldsTest::compareFields, "FieldCmp"))
        .containsExactlyElementsIn(Arrays.asList(expected));
  }

  @Test
  public void testOutOfBoundsLineIsNotIndexed() {
    Field[] fields = shapeFields(MockGeometries.OUT_OF_BOUNDS_LINE);
    Assert.assertEquals(0, fields.length);
  }

  @Test
  public void testValidPolygonIsIndexed() {
    Field[] fields = shapeFields(MockGeometries.POLYGON);
    Assert.assertEquals(1, fields.length);
  }

  @Test
  public void testSelfIntersectingPolygonIsNotIndexed() {
    Field[] fields = shapeFields(MockGeometries.SELF_INTERSECTING_POLYGON);
    Assert.assertEquals(0, fields.length);
  }

  private IndexMetricsUpdater.IndexingMetricsUpdater createMockIndexingMetricsUpdater() {
    return new IndexMetricsUpdater.IndexingMetricsUpdater(
        SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH);
  }

  private Field[] shapeFields(Geometry shape) {
    return LuceneGeometryFields.forGeoShape(shape, FIELD_NAME, createMockIndexingMetricsUpdater())
        .toArray(Field[]::new);
  }

  private List<Field> pointFields(Geometry shape) {
    return LuceneGeometryFields.forGeoPoint(shape, FIELD_NAME, createMockIndexingMetricsUpdater())
        .collect(Collectors.toList());
  }
}
