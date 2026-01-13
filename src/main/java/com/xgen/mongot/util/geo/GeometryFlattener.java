package com.xgen.mongot.util.geo;

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import java.util.stream.Stream;

public class GeometryFlattener {

  private GeometryFlattener() {}

  /**
   * Produce Geometry leafs (Point, LineString, Polygon) by recursively visiting composite
   * geometries.
   */
  public static Stream<LeafGeometry> flatten(Geometry geometry) {
    GeometryFlattener flattener = new GeometryFlattener();
    return flattener.visit(geometry);
  }

  private Stream<LeafGeometry> visit(Geometry geometry) {
    return switch (geometry.getType()) {
      case GEOMETRY_COLLECTION -> visit((GeometryCollection) geometry);
      case LINE_STRING -> visit((LineString) geometry);
      case MULTI_LINE_STRING -> visit((MultiLineString) geometry);
      case MULTI_POINT -> visit((MultiPoint) geometry);
      case MULTI_POLYGON -> visit((MultiPolygon) geometry);
      case POINT -> visit((Point) geometry);
      case POLYGON -> visit((Polygon) geometry);
    };
  }

  private Stream<LeafGeometry> visit(GeometryCollection geometry) {
    return geometry.getGeometries().stream().flatMap(this::visit);
  }

  private Stream<LeafGeometry> visit(MultiPoint geometry) {
    return geometry.getCoordinates().stream().map(Point::new).flatMap(this::visit);
  }

  private Stream<LeafGeometry> visit(MultiLineString geometry) {
    return geometry.getCoordinates().stream().map(LineString::new).flatMap(this::visit);
  }

  private Stream<LeafGeometry> visit(MultiPolygon geometry) {
    return geometry.getCoordinates().stream().map(Polygon::new).flatMap(this::visit);
  }

  private Stream<LeafGeometry> visit(Point geometry) {
    return Stream.of(new LeafGeometry(geometry));
  }

  private Stream<LeafGeometry> visit(LineString geometry) {
    return Stream.of(new LeafGeometry(geometry));
  }

  private Stream<LeafGeometry> visit(Polygon geometry) {
    return Stream.of(new LeafGeometry(geometry));
  }
}
