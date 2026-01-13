package com.xgen.mongot.index.lucene.explain.timing;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.timers.TimingData;
import java.util.Comparator;
import java.util.Map;
import org.bson.BsonDocument;

public record ExplainTimingBreakdown(
    QueryExecutionArea context, QueryExecutionArea match, QueryExecutionArea score)
    implements DocumentEncodable, Comparable<ExplainTimingBreakdown> {
  static class Fields {
    static final Field.Required<QueryExecutionArea> CONTEXT =
        Field.builder("context")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<QueryExecutionArea> MATCH =
        Field.builder("match")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<QueryExecutionArea> SCORE =
        Field.builder("score")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .required();
  }

  /** Create an ExplainTimingBreakdown from stats gathered by the Profiler. */
  public static ExplainTimingBreakdown fromExecutionStats(
      Map<ExplainTimings.Type, TimingData> timings) {
    return new ExplainTimingBreakdown(
        QueryExecutionArea.contextAreaFor(timings),
        QueryExecutionArea.matchAreaFor(timings),
        QueryExecutionArea.scoreAreaFor(timings));
  }

  public static ExplainTimingBreakdown fromBson(DocumentParser parser) throws BsonParseException {
    return new ExplainTimingBreakdown(
        parser.getField(Fields.CONTEXT).unwrap(),
        parser.getField(Fields.MATCH).unwrap(),
        parser.getField(Fields.SCORE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CONTEXT, this.context)
        .field(Fields.MATCH, this.match)
        .field(Fields.SCORE, this.score)
        .build();
  }

  @Override
  public int compareTo(ExplainTimingBreakdown other) {
    return Comparator.comparing(ExplainTimingBreakdown::context)
        .thenComparing(ExplainTimingBreakdown::match)
        .thenComparing(ExplainTimingBreakdown::score)
        .compare(this, other);
  }
}
