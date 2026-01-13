package com.xgen.mongot.index.query.shapes;

import com.mongodb.client.model.geojson.Point;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.geo.GeoJsonParser;
import org.bson.BsonDocument;

public record Box(Point bottomLeft, Point topRight) implements Shape {

  private static class Fields {
    static final Field.Required<Point> BOTTOM_LEFT =
        Field.builder("bottomLeft")
            .classField(GeoJsonParser::parsePoint, GeoJsonParser::geometryToBson)
            .disallowUnknownFields()
            .required();
    static final Field.Required<Point> TOP_RIGHT =
        Field.builder("topRight")
            .classField(GeoJsonParser::parsePoint, GeoJsonParser::geometryToBson)
            .disallowUnknownFields()
            .required();
  }

  /** parse a Box. */
  public static Box fromBson(DocumentParser parser) throws BsonParseException {
    Point bottomLeft = parser.getField(Fields.BOTTOM_LEFT).unwrap();
    Point topRight = parser.getField(Fields.TOP_RIGHT).unwrap();
    if (bottomLeft.getPosition().getValues().get(0) >= topRight.getPosition().getValues().get(0)) {
      parser
          .getContext()
          .handleSemanticError("bottomLeft.longitude must be smaller than topRight.longitude");
    }
    if (bottomLeft.getPosition().getValues().get(1) >= topRight.getPosition().getValues().get(1)) {
      parser
          .getContext()
          .handleSemanticError("bottomLeft.latitude must be smaller than topRight.latitude");
    }

    return new Box(bottomLeft, topRight);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.BOTTOM_LEFT, this.bottomLeft)
        .field(Fields.TOP_RIGHT, this.topRight)
        .build();
  }
}
