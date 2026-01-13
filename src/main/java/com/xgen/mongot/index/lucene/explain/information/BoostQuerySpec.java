package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record BoostQuerySpec(QueryExplainInformation query, float boost)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<QueryExplainInformation> QUERY =
        Field.builder("query")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<Float> BOOST = Field.builder("boost").floatField().required();
  }

  static BoostQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new BoostQuerySpec(
        parser.getField(Fields.QUERY).unwrap(), parser.getField(Fields.BOOST).unwrap());
  }

  @Override
  public Type getType() {
    return Type.BOOST_QUERY;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.QUERY, this.query)
        .field(Fields.BOOST, this.boost)
        .build();
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType() && equals((BoostQuerySpec) other, explainInfoEquator);
  }

  private boolean equals(BoostQuerySpec other, Equator<QueryExplainInformation> equator) {
    return Objects.equals(this.boost, other.boost) && equator.equate(this.query, other.query);
  }
}
