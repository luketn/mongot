package com.xgen.testing.mongot.util.geo;

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.xgen.mongot.util.BsonUtils;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.io.BasicOutputBuffer;

@SuppressWarnings("MutablePublicArray")
public class MockGeometries {

  public static final Position POS_A = new Position(10.0, 12.0);
  public static final Point POINT_A = new Point(POS_A);

  public static final Position POS_B = new Position(11.0, 13.0);
  public static final Point POINT_B = new Point(POS_B);

  public static final Point OUT_OF_BOUNDS_POINT = new Point(new Position(400.0, 13.0));

  public static final LineString LINE_AB = new LineString(list(POS_A, POS_B));
  public static final double[] LINE_LATS = new double[] {12.0, 13.0};
  public static final double[] LINE_LONS = new double[] {10.0, 11.0};

  public static final LineString OUT_OF_BOUNDS_LINE =
      new LineString(list(new Position(10, 10), new Position(10, 500)));

  public static final Polygon POLYGON =
      new Polygon(
          list(
              new Position(10.0, 12.0),
              new Position(11.0, 12.0),
              new Position(11.0, 11.0),
              new Position(10.0, 12.0)));

  public static final Polygon SELF_INTERSECTING_POLYGON =
      new Polygon(
          list(
              new Position(0.0, 0.0),
              new Position(1.0, 1.0),
              new Position(0.0, 1.0),
              new Position(1.0, 0.0),
              new Position(0.0, 0.0)));
  public static final double[] POLYGON_LONS = new double[] {10, 11, 11, 10};
  public static final double[] POLYGON_LATS = new double[] {12, 12, 11, 12};

  // contained in POLYGON:
  public static final Polygon INNER_POLYGON =
      new Polygon(
          list(
              new Position(10.10, 11.12),
              new Position(10.11, 11.12),
              new Position(10.11, 11.11),
              new Position(10.10, 11.12)));
  public static final double[] INNER_POLYGON_LONS = new double[] {10.10, 10.11, 10.11, 10.10};
  public static final double[] INNER_POLYGON_LATS = new double[] {11.12, 11.12, 11.11, 11.12};

  public static final GeometryCollection COLLECTION_POLYGON_LINE_POINT =
      collection(POLYGON, LINE_AB, POINT_A);
  public static final BsonDocument BSON_COLLECTION = serialize(COLLECTION_POLYGON_LINE_POINT);

  public static final BsonDocument BSON_POINT = serialize(POINT_A); // lon=10, lat=12
  public static final BsonDocument BSON_OUT_OF_BOUNDS_POINT = serialize(OUT_OF_BOUNDS_POINT);

  public static final BsonDocument BSON_LINE = serialize(LINE_AB); // lon=10, lat=12
  public static final BsonDocument BSON_POLYGON = serialize(POLYGON);

  public static final MultiPolygon MULTI_POLYGON =
      new MultiPolygon(new ArrayList<>(List.of(MockGeometries.POLYGON.getCoordinates())));
  public static final BsonDocument BSON_MULTIPOLYGON = serialize(MULTI_POLYGON);

  @SafeVarargs
  public static <T> List<T> list(T... elements) {
    // Must use ArrayLists with geojson package: JAVA-3635
    return new ArrayList<>(List.of(elements));
  }

  public static GeometryCollection collection(Geometry... elements) {
    return new GeometryCollection(list(elements));
  }

  public static MultiPoint multiPoint(Point... points) {
    List<Position> positions = list(points).stream().map(Point::getPosition).toList();
    return new MultiPoint(new ArrayList<>(positions));
  }

  public static MultiLineString multiLine(LineString... lines) {
    List<List<Position>> positions = list(lines).stream().map(LineString::getCoordinates).toList();
    return new MultiLineString(new ArrayList<>(positions));
  }

  /** serialize a geometry. */
  public static BsonDocument serialize(Geometry geometry) {
    var codec = CodecRegistries.fromProviders(new GeoJsonCodecProvider()).get(Geometry.class);
    BasicOutputBuffer buffer = new BasicOutputBuffer(1024);
    codec.encode(new BsonBinaryWriter(buffer), geometry, BsonUtils.DEFAULT_FAST_CONTEXT);
    return new RawBsonDocument(buffer.getInternalBuffer());
  }
}
