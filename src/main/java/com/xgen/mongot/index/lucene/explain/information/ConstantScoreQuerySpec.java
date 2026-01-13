package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record ConstantScoreQuerySpec(QueryExplainInformation query)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<QueryExplainInformation> QUERY =
        Field.builder("query")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .required();
  }

  static ConstantScoreQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new ConstantScoreQuerySpec(parser.getField(Fields.QUERY).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.QUERY, this.query).build();
  }

  @Override
  public Type getType() {
    return Type.CONSTANT_SCORE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType()
        && equals((ConstantScoreQuerySpec) other, explainInfoEquator);
  }

  private boolean equals(
      ConstantScoreQuerySpec other, Equator<QueryExplainInformation> explainInfoEquator) {
    return explainInfoEquator.equate(this.query, other.query);
  }
}
