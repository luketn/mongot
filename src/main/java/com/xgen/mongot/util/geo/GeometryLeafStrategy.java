package com.xgen.mongot.util.geo;

import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;

public interface GeometryLeafStrategy<E extends Exception> {

  void visit(Point point) throws E;

  void visit(LineString line) throws E;

  void visit(Polygon polygon) throws E;
}
