package com.xgen.mongot.index.query.points;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.geo.GeoJsonParser;
import org.bson.BsonValue;

public record GeoPoint(com.mongodb.client.model.geojson.Point value) implements Point {

  /** Deserializes a Point from BSON. */
  public static GeoPoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    switch (value.getBsonType()) {
      case DOCUMENT -> {
        try (BsonDocumentParser parser =
            BsonDocumentParser.withContext(context, value.asDocument()).build()) {
          return new GeoPoint(GeoJsonParser.parsePoint(parser));
        }
      }
      default -> {
        return context.handleUnexpectedType("geoPoint", value.getBsonType());
      }
    }
  }

  @Override
  public Type getType() {
    return Type.GEO;
  }

  @Override
  public String toString() {
    return value().toString();
  }

  @Override
  public BsonValue toBson() {
    return GeoJsonParser.geometryToBson(this.value);
  }
}
