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
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record SynonymQuerySpec(Optional<FieldPath> path, List<String> values)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();

    static final Field.Required<List<String>> VALUES =
        Field.builder("values")
            .listOf(Value.builder().stringValue().mustNotBeEmpty().required())
            .required();
  }

  static SynonymQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new SynonymQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.VALUES, this.values)
        .build();
  }

  @Override
  public Type getType() {
    return Type.SYNONYM_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    SynonymQuerySpec query = (SynonymQuerySpec) other;
    return Objects.equals(this.path, query.path) && Objects.equals(this.values, query.values);
  }
}
