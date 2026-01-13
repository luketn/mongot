package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.apache.lucene.index.Term;
import org.bson.BsonDocument;

public record TermQuerySpec(FieldPath path, String value) implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    static final Field.Required<String> VALUE = Field.builder("value").stringField().required();
  }

  public TermQuerySpec(Term term) {
    this(FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(term.field())), term.text());
  }

  static TermQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new TermQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.VALUE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.VALUE, this.value)
        .build();
  }

  @Override
  public Type getType() {
    return Type.TERM_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    TermQuerySpec query = (TermQuerySpec) other;
    return Objects.equals(this.path, query.path) && Objects.equals(this.value, query.value);
  }
}
