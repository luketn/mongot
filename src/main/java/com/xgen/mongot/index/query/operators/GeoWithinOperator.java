package com.xgen.mongot.index.query.operators;

import com.mongodb.client.model.geojson.GeoJsonObjectType;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.shapes.Box;
import com.xgen.mongot.index.query.shapes.Circle;
import com.xgen.mongot.index.query.shapes.GeometryShape;
import com.xgen.mongot.index.query.shapes.Shape;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record GeoWithinOperator(Score score, List<FieldPath> paths, Shape shape)
    implements Operator {
  private static class Fields {
    static final Field.Optional<Circle> CIRCLE =
        Field.builder("circle")
            .classField(Circle::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Optional<Box> BOX =
        Field.builder("box")
            .classField(Box::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Optional<GeometryShape> GEOMETRY_SHAPE =
        Field.builder("geometry")
            .classField(GeometryShape::fromBson)
            .disallowUnknownFields()
            .validate(Fields::isPolygon)
            .optional()
            .noDefault();

    private static Optional<String> isPolygon(GeometryShape geometryShape) {
      GeoJsonObjectType type = geometryShape.geometry().getType();
      return switch (type) {
        case POLYGON, MULTI_POLYGON -> Optional.empty();
        default -> Optional.of("Geometry must be a Polygon");
      };
    }
  }

  /** Deserializes a GeoWithinOperator from the supplied DocumentParser. */
  public static GeoWithinOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new GeoWithinOperator(
        Operators.parseScore(parser), Operators.parseFieldPath(parser), parseShape(parser));
  }

  @Override
  public BsonValue operatorToBson() {
    BsonDocumentBuilder builder = Operators.documentBuilder(score(), this.paths);
    return switch (this.shape) {
      case Box box -> builder.field(Fields.BOX, Optional.of(box)).build();
      case Circle circle -> builder.field(Fields.CIRCLE, Optional.of(circle)).build();
      case GeometryShape geometryShape ->
          builder.field(Fields.GEOMETRY_SHAPE, Optional.of(geometryShape)).build();
    };
  }

  private static Shape parseShape(DocumentParser parser) throws BsonParseException {
    return parser
        .getGroup()
        .exactlyOneOf(
            parser.getField(Fields.CIRCLE),
            parser.getField(Fields.BOX),
            parser.getField(Fields.GEOMETRY_SHAPE));
  }

  @Override
  public Type getType() {
    return Type.GEO_WITHIN;
  }
}
