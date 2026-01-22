package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

/*
Ideally we would be able to extract the baseQuery and drillDownQueries from the query. However,
this is not possible here because Lucene's DrillSidewaysQuery is package-private so we
do not have access to these elements. This limitation could be addressed by wrapping
DrillSidewaysQuery in a future improvement.
 */
public record DrillSidewaysQuerySpec() implements LuceneQuerySpecification {

  private static final String QUERY_TYPE_VALUE = "DrillSidewaysQuery";

  static class Fields {
    static final Field.Required<String> QUERY_TYPE =
        Field.builder("queryType").stringField().required();
  }

  static DrillSidewaysQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    parser.getField(Fields.QUERY_TYPE).unwrap();
    return new DrillSidewaysQuerySpec();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.QUERY_TYPE, QUERY_TYPE_VALUE).build();
  }

  @Override
  public Type getType() {
    return Type.DRILL_SIDEWAYS_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType();
  }
}
