package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record MultiPhraseQuerySpec(FieldPath path, String query, int slop)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    static final Field.Required<String> QUERY = Field.builder("query").stringField().required();

    static final Field.Required<Integer> SLOP = Field.builder("slop").intField().required();
  }

  static MultiPhraseQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new MultiPhraseQuerySpec(
        parser.getField(Fields.PATH).unwrap(),
        parser.getField(Fields.QUERY).unwrap(),
        parser.getField(Fields.SLOP).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.QUERY, this.query)
        .field(Fields.SLOP, this.slop)
        .build();
  }

  @Override
  public Type getType() {
    return Type.MULTI_PHRASE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    MultiPhraseQuerySpec query = (MultiPhraseQuerySpec) other;
    return Objects.equals(this.path, query.path)
        && Objects.equals(this.query, query.query)
        && Objects.equals(this.slop, query.slop);
  }
}
