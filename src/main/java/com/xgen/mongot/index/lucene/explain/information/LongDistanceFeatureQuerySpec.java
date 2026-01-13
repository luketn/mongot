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

// Ideally, path and representation would be required fields/would not be optional here.
// These are optional as because we're not always being able to get the path from
// LongDistanceFeatureQuery::toString - we don't have a good way of getting the path from the
// lucene query, and without the path we can't infer the representation. See
// https://github.com/apache/lucene-solr/blob/7a301c736c4da9629602c8be752fd43a85224768/lucene/core/src/java/org/apache/lucene/document/LongDistanceFeatureQuery.java#L82-L92
// to understand why we can't easily get the path.
public record LongDistanceFeatureQuerySpec(
    Optional<FieldPath> path,
    Optional<Point> origin,
    Optional<Point> pivotDistance,
    Optional<Representation> representation)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();

    static final Field.Optional<Point> ORIGIN =
        Field.builder("origin").classField(Point::fromBson).optional().noDefault();

    static final Field.Optional<Point> PIVOT_DISTANCE =
        Field.builder("pivotDistance").classField(Point::fromBson).optional().noDefault();

    static final Field.Optional<Representation> REPRESENTATION =
        Field.builder("representation")
            .enumField(Representation.class)
            .asCamelCase()
            .optional()
            .noDefault();
  }

  public LongDistanceFeatureQuerySpec() {
    this(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  static LongDistanceFeatureQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new LongDistanceFeatureQuerySpec(
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.ORIGIN).unwrap(),
        parser.getField(Fields.PIVOT_DISTANCE).unwrap(),
        parser.getField(Fields.REPRESENTATION).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.ORIGIN, this.origin)
        .field(Fields.PIVOT_DISTANCE, this.pivotDistance)
        .field(Fields.REPRESENTATION, this.representation)
        .build();
  }

  @Override
  public Type getType() {
    return Type.LONG_DISTANCE_FEATURE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    LongDistanceFeatureQuerySpec otherQuery = (LongDistanceFeatureQuerySpec) other;
    return Objects.equals(this.path, otherQuery.path)
        && Objects.equals(this.origin, otherQuery.origin)
        && Objects.equals(this.pivotDistance, otherQuery.pivotDistance)
        && Objects.equals(this.representation, otherQuery.representation);
  }
}
