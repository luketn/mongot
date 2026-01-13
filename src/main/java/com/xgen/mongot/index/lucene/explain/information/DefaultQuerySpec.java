package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record DefaultQuerySpec(String queryType) implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<String> QUERY_TYPE =
        Field.builder("queryType").stringField().required();
  }

  static DefaultQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new DefaultQuerySpec(parser.getField(Fields.QUERY_TYPE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.QUERY_TYPE, this.queryType).build();
  }

  @Override
  public Type getType() {
    return Type.DEFAULT_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    DefaultQuerySpec otherDefault = (DefaultQuerySpec) other;
    return Objects.equals(this.queryType, otherDefault.queryType);
  }
}
