package com.xgen.mongot.index.query.shapes;

import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.geo.GeoJsonParser;
import org.bson.BsonDocument;

public record Circle(Point center, double radiusMeters) implements Shape {

  private static class Fields {
    static final Field.Required<Point> CENTER =
        Field.builder("center")
            .classField(GeoJsonParser::parsePoint, GeoJsonParser::geometryToBson)
            .disallowUnknownFields()
            .required();
    static final Field.Required<Double> RADIUS =
        Field.builder("radius").doubleField().mustBeFinite().mustBeNonNegative().required();
  }

  public Circle(double centerLon, double centerLat, double radiusMeters) {
    this(new Point(new Position(centerLon, centerLat)), radiusMeters);
  }

  public static Circle fromBson(DocumentParser parser) throws BsonParseException {
    return new Circle(
        parser.getField(Fields.CENTER).unwrap(), parser.getField(Fields.RADIUS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CENTER, this.center)
        .field(Fields.RADIUS, this.radiusMeters)
        .build();
  }
}
