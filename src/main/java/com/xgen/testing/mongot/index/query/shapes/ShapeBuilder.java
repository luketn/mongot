package com.xgen.testing.mongot.index.query.shapes;

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.xgen.mongot.index.query.shapes.Box;
import com.xgen.mongot.index.query.shapes.Circle;
import com.xgen.mongot.index.query.shapes.GeometryShape;
import java.util.ArrayList;
import java.util.List;

public class ShapeBuilder {

  public static Circle circle(double centerLon, double centerLat, double radiusMeters) {
    return new Circle(centerLon, centerLat, radiusMeters);
  }

  public static Box box(double bottomLon, double bottomLat, double topLon, double topLat) {
    return new Box(buildPoint(bottomLon, bottomLat), buildPoint(topLon, topLat));
  }

  public static GeometryShape geometry(Geometry geometry) {
    return new GeometryShape(geometry);
  }

  private static Point buildPoint(double lon, double lat) {
    return new Point(new Position(new ArrayList<>(List.of(lon, lat))));
  }
}
