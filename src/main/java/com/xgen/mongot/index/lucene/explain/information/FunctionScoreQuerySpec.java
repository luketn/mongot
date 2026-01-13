package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record FunctionScoreQuerySpec(String scoreFunction, QueryExplainInformation query)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<String> SCORE_FUNCTION =
        Field.builder("scoreFunction").stringField().required();

    static final Field.Required<QueryExplainInformation> QUERY =
        Field.builder("query")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .required();
  }

  static FunctionScoreQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new FunctionScoreQuerySpec(
        parser.getField(Fields.SCORE_FUNCTION).unwrap(), parser.getField(Fields.QUERY).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.SCORE_FUNCTION, this.scoreFunction)
        .field(Fields.QUERY, this.query)
        .build();
  }

  @Override
  public Type getType() {
    return Type.FUNCTION_SCORE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType()
        && equals((FunctionScoreQuerySpec) other, explainInfoEquator);
  }

  private boolean equals(
      FunctionScoreQuerySpec other, Equator<QueryExplainInformation> explainInfoEquator) {
    return Objects.equals(this.scoreFunction, other.scoreFunction)
        && explainInfoEquator.equate(this.query, other.query);
  }
}
