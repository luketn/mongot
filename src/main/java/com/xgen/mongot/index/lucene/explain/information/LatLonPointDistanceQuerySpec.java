package com.xgen.mongot.index.lucene.explain.information;

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

public record LatLonPointDistanceQuerySpec(
    Optional<FieldPath> path, Optional<Coordinate> center, Optional<Double> radius)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();

    static final Field.Optional<Coordinate> CENTER =
        Field.builder("center").classField(Coordinate::fromBson).optional().noDefault();

    static final Field.Optional<Double> RADIUS =
        Field.builder("radius").doubleField().optional().noDefault();
  }

  public LatLonPointDistanceQuerySpec() {
    this(Optional.empty(), Optional.empty(), Optional.empty());
  }

  static LatLonPointDistanceQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new LatLonPointDistanceQuerySpec(
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.CENTER).unwrap(),
        parser.getField(Fields.RADIUS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.CENTER, this.center)
        .field(Fields.RADIUS, this.radius)
        .build();
  }

  @Override
  public Type getType() {
    return Type.LAT_LON_POINT_DISTANCE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType() && equals((LatLonPointDistanceQuerySpec) other);
  }

  private boolean equals(LatLonPointDistanceQuerySpec other) {
    return Objects.equals(this.path, other.path)
        && Objects.equals(this.center, other.center)
        && Objects.equals(this.radius, other.radius);
  }
}
