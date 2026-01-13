package com.xgen.mongot.util.geo;

import com.mongodb.client.model.geojson.GeoJsonObjectType;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;

/** Represents a geometry leaf. */
public class LeafGeometry {

  private final Geometry geometry;

  public LeafGeometry(Polygon geometry) {
    this.geometry = geometry;
  }

  public LeafGeometry(LineString geometry) {
    this.geometry = geometry;
  }

  public LeafGeometry(Point geometry) {
    this.geometry = geometry;
  }

  public GeoJsonObjectType getType() {
    return this.geometry.getType();
  }

  public Point asPoint() {
    throwIfInvalidType(GeoJsonObjectType.POINT);
    return (Point) this.geometry;
  }

  public LineString asLineString() {
    throwIfInvalidType(GeoJsonObjectType.LINE_STRING);
    return (LineString) this.geometry;
  }

  public Polygon asPolygon() {
    throwIfInvalidType(GeoJsonObjectType.POLYGON);
    return (Polygon) this.geometry;
  }

  private void throwIfInvalidType(GeoJsonObjectType expectedType) {
    if (this.geometry.getType() != expectedType) {
      throw new UnsupportedOperationException(
          String.format("Expected geometry type to be: %s, but was %s", expectedType, getType()));
    }
  }
}
