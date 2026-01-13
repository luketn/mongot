package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record LatLonShapeQuerySpec(
    Optional<FieldPath> path, Optional<List<List<Coordinate>>> coordinates)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();

    static final Field.Optional<List<List<Coordinate>>> COORDS =
        Field.builder("coordinates")
            .classField(Coordinate::fromBson)
            .asList()
            .asList()
            .optional()
            .noDefault();
  }

  public LatLonShapeQuerySpec() {
    this(Optional.empty(), Optional.empty());
  }

  static LatLonShapeQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new LatLonShapeQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.COORDS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.COORDS, this.coordinates)
        .build();
  }

  @Override
  public Type getType() {
    return Type.LAT_LON_SHAPE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    LatLonShapeQuerySpec otherQuery = (LatLonShapeQuerySpec) other;
    return Objects.equals(this.path, otherQuery.path)
        && Objects.equals(this.coordinates, otherQuery.coordinates);
  }
}
