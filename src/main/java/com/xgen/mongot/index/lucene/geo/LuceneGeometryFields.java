package com.xgen.mongot.index.lucene.geo;

import com.google.common.flogger.FluentLogger;
import com.mongodb.client.model.geojson.GeoJsonObjectType;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.util.geo.GeometryFlattener;
import com.xgen.mongot.util.geo.LeafGeometry;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LatLonShape;

public class LuceneGeometryFields {

  private static final FluentLogger flogger = FluentLogger.forEnclosingClass();

  private final String fieldName;

  private final IndexingMetricsUpdater indexingMetricsUpdater;

  private LuceneGeometryFields(String fieldName, IndexingMetricsUpdater indexMetricsUpdater) {
    this.fieldName = fieldName;
    this.indexingMetricsUpdater = indexMetricsUpdater;
  }

  /**
   * Produce the lucene fields to index a Point, Points nested under MultiPoint and
   * GeometryCollection are also processed.
   */
  public static Stream<Field> forGeoPoint(
      Geometry geometry, String fieldName, IndexingMetricsUpdater indexingMetricsUpdater) {
    // only points are considered here.
    return GeometryFlattener.flatten(geometry)
        .filter(leafGeometry -> leafGeometry.getType() == GeoJsonObjectType.POINT)
        .map(LeafGeometry::asPoint)
        .flatMap(new LuceneGeometryFields(fieldName, indexingMetricsUpdater)::pointAsPoint);
  }

  /** Produce the lucene fields to index a geoShape field. */
  public static Stream<Field> forGeoShape(
      Geometry geometry, String fieldName, IndexingMetricsUpdater indexingMetricsUpdater) {
    return GeometryFlattener.flatten(geometry)
        .flatMap(new LuceneGeometryFields(fieldName, indexingMetricsUpdater)::geometryAsShape);
  }

  private Stream<Field> pointAsPoint(Point point) {
    try {
      org.apache.lucene.geo.Point lucenePoint = LuceneGeoTranslator.point(point);
      // two LatLon fields are required for LatLonPoint.newDistanceFeatureQuery().
      return Stream.of(
          new LatLonPoint(this.fieldName, lucenePoint.getLat(), lucenePoint.getLon()),
          new LatLonDocValuesField(this.fieldName, lucenePoint.getLat(), lucenePoint.getLon()));
    } catch (InvalidGeoPosition e) {
      return Stream.empty();
    }
  }

  private Stream<Field> geometryAsShape(LeafGeometry leafGeometry) {
    // Append to field the arrays for each shape visited
    try {
      return Arrays.stream(unsafeShapeFields(leafGeometry));
    } catch (InvalidGeoPosition e) {
      // silently ignore.
      return Stream.empty();
    } catch (IllegalArgumentException e) {
      this.indexingMetricsUpdater.getInvalidGeometryFieldCounter().increment();
      flogger.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
          "Error converting a LeafGeometry to a Lucene Field. fieldName %s", this.fieldName);
      return Stream.empty();
    }
  }

  private Field[] unsafeShapeFields(LeafGeometry leafGeometry) throws InvalidGeoPosition {
    return switch (leafGeometry.getType()) {
      case POINT -> {
        org.apache.lucene.geo.Point point = LuceneGeoTranslator.point(leafGeometry.asPoint());
        yield LatLonShape.createIndexableFields(this.fieldName, point.getLat(), point.getLon());
      }
      case LINE_STRING -> {
        LineString lineString = leafGeometry.asLineString();
        yield LatLonShape.createIndexableFields(
            this.fieldName, LuceneGeoTranslator.line(lineString));
      }
      case POLYGON -> {
        Polygon polygon = leafGeometry.asPolygon();
        yield LatLonShape.createIndexableFields(
            this.fieldName, LuceneGeoTranslator.polygon(polygon));
      }
      default -> throw new IllegalStateException("Unexpected value: " + leafGeometry.getType());
    };
  }
}
