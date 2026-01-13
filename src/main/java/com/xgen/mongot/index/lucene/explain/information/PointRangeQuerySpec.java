package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record PointRangeQuerySpec(
    FieldPath path,
    Optional<Representation> representation,
    Optional<Point> greaterThanEq,
    Optional<Point> lessThanEq)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    static final Field.Optional<Representation> REPRESENTATION =
        Field.builder("representation")
            .enumField(Representation.class)
            .asCamelCase()
            .optional()
            .noDefault();

    static final Field.Optional<Point> GREATER_THAN_EQ =
        Field.builder("gte").classField(Point::fromBson).optional().noDefault();

    static final Field.Optional<Point> LESS_THAN_EQ =
        Field.builder("lte").classField(Point::fromBson).optional().noDefault();
  }

  static PointRangeQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new PointRangeQuerySpec(
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.REPRESENTATION).unwrap(),
        parser.getField(Fields.GREATER_THAN_EQ).unwrap(),
        parser.getField(Fields.LESS_THAN_EQ).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.REPRESENTATION, this.representation)
        .field(Fields.GREATER_THAN_EQ, this.greaterThanEq)
        .field(Fields.LESS_THAN_EQ, this.lessThanEq)
        .build();
  }

  @Override
  public Type getType() {
    return Type.POINT_RANGE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> ignored1,
      Comparator<QueryExplainInformation> ignored2) {
    if (other.getType() != getType()) {
      return false;
    }

    PointRangeQuerySpec that = (PointRangeQuerySpec) other;
    return Objects.equals(this.path, that.path)
        && Objects.equals(this.representation, that.representation)
        && Objects.equals(this.greaterThanEq, that.greaterThanEq)
        && Objects.equals(this.lessThanEq, that.lessThanEq);
  }
}
