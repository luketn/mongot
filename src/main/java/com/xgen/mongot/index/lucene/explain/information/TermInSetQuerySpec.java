package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record TermInSetQuerySpec(FieldPath path, List<String> stringSet)
    implements LuceneQuerySpecification {

  private static class Fields {
    private static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    private static final Field.Required<List<String>> VALUES =
        Field.builder("values").listOf(Value.builder().stringValue().required()).required();
  }

  static TermInSetQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new TermInSetQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUES).unwrap());
  }

  @Override
  public Type getType() {
    return Type.TERM_IN_SET_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }
    TermInSetQuerySpec query = (TermInSetQuerySpec) other;
    return Objects.equals(this.path, query.path) && Objects.equals(this.stringSet, query.stringSet);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.VALUES, this.stringSet)
        .build();
  }
}
