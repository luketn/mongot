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
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record PointInSetQuerySpec(FieldPath path, List<Long> points)
    implements LuceneQuerySpecification {

  private static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
    static final Field.Required<List<Long>> POINTS =
        Field.builder("points").longField().asList().required();
  }

  static PointInSetQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new PointInSetQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.POINTS).unwrap());
  }

  @Override
  public Type getType() {
    return Type.POINT_IN_SET_QUERY;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.POINTS, this.points)
        .build();
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> equator,
      Comparator<QueryExplainInformation> comparator) {
    if (other.getType() != Type.POINT_IN_SET_QUERY) {
      return false;
    }
    var query = (PointInSetQuerySpec) other;

    return Objects.equals(this.path, query.path) && Objects.equals(this.points, query.points);
  }
}
