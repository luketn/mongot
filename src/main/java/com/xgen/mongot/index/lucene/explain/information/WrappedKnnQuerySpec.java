package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record WrappedKnnQuerySpec(
    List<QueryExplainInformation> query, Optional<QueryExplainInformation> filter)
    implements LuceneQuerySpecification {

  static class Fields {
    static final Field.Required<List<QueryExplainInformation>> QUERY =
        Field.builder("query")
            .listOf(
                Value.builder()
                    .classValue(QueryExplainInformation::fromBson)
                    .disallowUnknownFields()
                    .required())
            .required();

    static final Field.Optional<QueryExplainInformation> FILTER =
        Field.builder("filter")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static WrappedKnnQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new WrappedKnnQuerySpec(
        parser.getField(Fields.QUERY).unwrap(), parser.getField(Fields.FILTER).unwrap());
  }

  @Override
  public Type getType() {
    return Type.WRAPPED_KNN_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (!(other.getType() == getType())) {
      return false;
    }
    var otherQuery = (WrappedKnnQuerySpec) other;

    if (otherQuery.query.size() != this.query.size()) {
      return false;
    }

    for (var i = 0; i < this.query.size(); i++) {
      if (!explainInfoEquator.equate(this.query.get(i), otherQuery.query.get(i))) {
        return false;
      }
    }

    if (this.filter.isPresent() != otherQuery.filter.isPresent()) {
      return false;
    }

    return this.filter
        .map(
            luceneExplainInformation ->
                explainInfoEquator.equate(luceneExplainInformation, otherQuery.filter.get()))
        .orElse(true);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FILTER, this.filter)
        .field(Fields.QUERY, this.query)
        .build();
  }
}
