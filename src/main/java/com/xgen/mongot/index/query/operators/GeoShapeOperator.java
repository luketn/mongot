package com.xgen.mongot.index.query.operators;

import com.mongodb.client.model.geojson.GeoJsonObjectType;
import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.shapes.GeometryShape;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.geo.GeometryFlattener;
import com.xgen.mongot.util.geo.LeafGeometry;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.BsonValue;

public record GeoShapeOperator(
    Score score, List<FieldPath> paths, GeometryShape geometryShape, Relation relation)
    implements Operator {

  public enum Relation {
    CONTAINS,
    DISJOINT,
    INTERSECTS,
    WITHIN,
  }

  private static class Fields {
    static final Field.Required<Relation> RELATION =
        Field.builder("relation").enumField(Relation.class).asCamelCase().required();
    static final Field.Required<GeometryShape> GEOMETRY =
        Field.builder("geometry")
            .classField(GeometryShape::fromBson)
            .disallowUnknownFields()
            .required();
  }

  public static GeoShapeOperator fromBson(DocumentParser parser) throws BsonParseException {
    var shape = parser.getField(Fields.GEOMETRY).unwrap();
    var relation = parser.getField(Fields.RELATION).unwrap();

    validShapeAndRelations(parser.getContext(), relation, shape.geometry());

    return new GeoShapeOperator(
        Operators.parseScore(parser), Operators.parseFieldPath(parser), shape, relation);
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score(), this.paths)
        .field(Fields.RELATION, this.relation)
        .field(Fields.GEOMETRY, this.geometryShape)
        .build();
  }

  @Override
  public Type getType() {
    return Type.GEO_SHAPE;
  }

  private static void validShapeAndRelations(
      BsonParseContext context, Relation relation, Geometry geometry) throws BsonParseException {

    Set<GeoJsonObjectType> geometryTypes =
        GeometryFlattener.flatten(geometry)
            .map(LeafGeometry::getType)
            .collect(Collectors.toUnmodifiableSet());

    if (geometryTypes.isEmpty()) {
      context.handleSemanticError("must be more than one geometry present");
    }

    if (relation == Relation.WITHIN) {
      if (geometryTypes.contains(GeoJsonObjectType.POINT)) {
        context.handleSemanticError("cannot use relation 'within' with points");
      }
      if (geometryTypes.contains(GeoJsonObjectType.LINE_STRING)) {
        context.handleSemanticError("cannot use relation 'within' with lines");
      }
    }
  }
}
