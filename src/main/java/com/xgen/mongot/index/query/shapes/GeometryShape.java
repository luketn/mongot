package com.xgen.mongot.index.query.shapes;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.geo.GeoJsonParser;
import org.bson.BsonDocument;

/** A Shape representing any GeoJson Geometry. */
public record GeometryShape(Geometry geometry) implements Shape {

  public static GeometryShape fromBson(DocumentParser parser) throws BsonParseException {
    Geometry geometry = GeoJsonParser.parseGeometry(parser);
    return new GeometryShape(geometry);
  }

  @Override
  public BsonDocument toBson() {
    return GeoJsonParser.geometryToBson(this.geometry);
  }
}
