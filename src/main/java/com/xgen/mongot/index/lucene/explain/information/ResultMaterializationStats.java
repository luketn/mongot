package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record ResultMaterializationStats(QueryExecutionArea stats)
    implements DocumentEncodable,
        EqualsWithTimingEquator<ResultMaterializationStats>,
        Comparable<ResultMaterializationStats> {
  static class Fields {
    static final Field.Required<QueryExecutionArea> STATS =
        Field.builder("stats")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .required();
  }

  public static ResultMaterializationStats fromBson(DocumentParser parser)
      throws BsonParseException {
    return new ResultMaterializationStats(parser.getField(Fields.STATS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.STATS, this.stats).build();
  }

  @Override
  public boolean equals(
      ResultMaterializationStats other, Equator<QueryExecutionArea> timingEquator) {
    return timingEquator.equate(this.stats, other.stats);
  }

  @Override
  public int compareTo(ResultMaterializationStats o) {
    return this.stats.compareTo(o.stats);
  }
}
