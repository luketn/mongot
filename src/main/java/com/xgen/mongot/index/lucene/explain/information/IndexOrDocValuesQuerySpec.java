package com.xgen.mongot.index.lucene.explain.information;

import static com.xgen.mongot.index.lucene.explain.information.BooleanQuerySpec.clauseListEquals;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record IndexOrDocValuesQuerySpec(Optional<List<QueryExplainInformation>> query)
    implements LuceneQuerySpecification {

  private static class Fields {
    static final Field.Optional<List<QueryExplainInformation>> QUERY =
        Field.builder("query")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  static IndexOrDocValuesQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexOrDocValuesQuerySpec(
        parser.getField(IndexOrDocValuesQuerySpec.Fields.QUERY).unwrap());
  }

  @Override
  public Type getType() {
    return Type.INDEX_OR_DOC_VALUES_QUERY;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(IndexOrDocValuesQuerySpec.Fields.QUERY, this.query)
        .build();
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> equator,
      Comparator<QueryExplainInformation> comparator) {
    if (other.getType() != Type.INDEX_OR_DOC_VALUES_QUERY) {
      return false;
    }
    var otherSpec = (IndexOrDocValuesQuerySpec) other;
    return clauseListEquals(this.query, otherSpec.query, equator, comparator);
  }
}
