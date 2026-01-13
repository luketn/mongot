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

public record MultiTermQueryConstantScoreBlendedWrapperSpec(
    Optional<FieldPath> path, List<QueryExplainInformation> queries)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();

    static final Field.Required<List<QueryExplainInformation>> QUERIES =
        Field.builder("queries")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  static MultiTermQueryConstantScoreBlendedWrapperSpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new MultiTermQueryConstantScoreBlendedWrapperSpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.QUERIES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.QUERIES, this.queries)
        .build();
  }

  @Override
  public Type getType() {
    return Type.MULTI_TERM_QUERY_CONSTANT_SCORE_BLENDED_WRAPPER;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    MultiTermQueryConstantScoreBlendedWrapperSpec query =
        (MultiTermQueryConstantScoreBlendedWrapperSpec) other;
    return Objects.equals(this.path, query.path)
        && BooleanQuerySpec.clauseListEquals(
            Optional.of(this.queries), Optional.of(query.queries), explainInfoEquator, childSorter);
  }
}
