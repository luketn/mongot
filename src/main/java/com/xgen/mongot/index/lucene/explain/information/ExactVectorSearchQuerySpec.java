package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.bson.BsonDocument;

public record ExactVectorSearchQuerySpec(
    String field,
    VectorSimilarityFunction similarityFunction,
    Optional<QueryExplainInformation> filter)
    implements LuceneQuerySpecification {

  static class Fields {
    static final Field.Optional<QueryExplainInformation> FILTER =
        Field.builder("filter")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Required<String> FIELD = Field.builder("field").stringField().required();

    static final Field.Required<VectorSimilarityFunction> SIMILARITY_FUNCTION =
        Field.builder("similarityFunction")
            .enumField(VectorSimilarityFunction.class)
            .asCamelCase()
            .required();
  }

  @Override
  public Type getType() {
    return Type.EXACT_VECTOR_SEARCH_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    ExactVectorSearchQuerySpec that = (ExactVectorSearchQuerySpec) other;
    return Objects.equals(this.field, that.field)
        && this.similarityFunction == that.similarityFunction
        && (this.filter.isEmpty() && that.filter.isEmpty()
            || (this.filter.isPresent()
                && that.filter.isPresent()
                && explainInfoEquator.equate(this.filter.get(), that.filter.get())));
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FIELD, this.field)
        .field(Fields.SIMILARITY_FUNCTION, this.similarityFunction)
        .field(Fields.FILTER, this.filter)
        .build();
  }

  public static ExactVectorSearchQuerySpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new ExactVectorSearchQuerySpec(
        parser.getField(Fields.FIELD).unwrap(),
        parser.getField(Fields.SIMILARITY_FUNCTION).unwrap(),
        parser.getField(Fields.FILTER).unwrap());
  }
}
