package com.xgen.mongot.index.query.points;

import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.TypeDescription;
import com.xgen.mongot.util.geo.GeoJsonParser;
import java.util.Date;
import org.bson.BsonBinarySubType;
import org.bson.BsonType;
import org.bson.BsonValue;

public sealed interface Point extends Encodable
    permits BooleanPoint, DatePoint, GeoPoint, NumericPoint, ObjectIdPoint, StringPoint, UuidPoint {

  enum Type {
    BOOLEAN,
    DATE,
    GEO,
    NUMBER,
    OBJECT_ID,
    STRING,
    UUID,
  }

  Type getType();

  @Override
  boolean equals(Object o);

  @Override
  int hashCode();

  static Point fromBson(BsonParseContext context, BsonValue value) throws BsonParseException {
    switch (value.getBsonType()) {
      case BOOLEAN:
        return new BooleanPoint(value.asBoolean().getValue());

      case DATE_TIME:
        return new DatePoint(new Date(value.asDateTime().getValue()));

      case DOUBLE:
        return new DoublePoint(value.asDouble().getValue());

      case INT32:
        return new LongPoint((long) value.asInt32().getValue());

      case INT64:
        return new LongPoint(value.asInt64().getValue());

      case OBJECT_ID:
        return new ObjectIdPoint(value.asObjectId().getValue());

      case BINARY:
        // NOTE: possible issue if value is not uuid type
        var binary = value.asBinary();
        if (binary.getType() != BsonBinarySubType.UUID_STANDARD.getValue()) {
          context.handleUnexpectedType(TypeDescription.UUID, BsonType.BINARY);
        }
        return new UuidPoint(binary.asUuid());

      case STRING:
        return new StringPoint(value.asString().getValue());

      case DOCUMENT:
        try (BsonDocumentParser parser =
            BsonDocumentParser.withContext(context, value.asDocument()).build()) {
          return new GeoPoint(GeoJsonParser.parsePoint(parser));
        }

      default:
        return context.handleSemanticError("Type is not supported: " + value.getBsonType());
    }
  }
}
