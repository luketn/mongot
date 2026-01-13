package com.xgen.mongot.index.lucene.explain.timing;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import com.xgen.mongot.util.timers.TimingData;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

public record QueryExecutionArea(double millisElapsed, Optional<Map<String, Long>> invocationCounts)
    implements DocumentEncodable, Comparable<QueryExecutionArea> {
  static class Fields {
    static final Field.Required<Double> MILLIS_ELAPSED =
        Field.builder("millisElapsed").doubleField().required();

    static final Field.Optional<Map<String, Long>> INVOCATION_COUNTS =
        Field.builder("invocationCounts")
            .mapOf(Value.builder().longValue().required())
            .optional()
            .noDefault();
  }

  private static final EnumSet<ExplainTimings.Type> CONTEXT_TIMING_TYPES =
      EnumSet.of(
          ExplainTimings.Type.VECTOR_EXECUTION,
          ExplainTimings.Type.CREATE_WEIGHT,
          ExplainTimings.Type.CREATE_SCORER);

  private static final EnumSet<ExplainTimings.Type> MATCH_TIMING_TYPES =
      EnumSet.of(ExplainTimings.Type.NEXT_DOC, ExplainTimings.Type.MATCH);

  private static final EnumSet<ExplainTimings.Type> SCORE_TIMING_TYPES =
      EnumSet.of(ExplainTimings.Type.SCORE, ExplainTimings.Type.SET_MIN_COMPETITIVE_SCORE);

  private static final EnumSet<ExplainTimings.Type> COLLECT_TIMING_TYPES =
      EnumSet.of(
          ExplainTimings.Type.COLLECT,
          ExplainTimings.Type.COMPETITIVE_ITERATOR,
          ExplainTimings.Type.SET_SCORER);

  private static final EnumSet<ExplainTimings.Type> SORT_COMPARATOR_TIMING_TYPES =
      EnumSet.of(
          ExplainTimings.Type.SET_BOTTOM,
          ExplainTimings.Type.COMPARE_BOTTOM,
          ExplainTimings.Type.COMPARE_TOP,
          ExplainTimings.Type.SET_HITS_THRESHOLD_REACHED,
          ExplainTimings.Type.COMPETITIVE_ITERATOR,
          ExplainTimings.Type.SET_SCORER);

  private static final EnumSet<ExplainTimings.Type> FACET_CREATE_COUNT_TIMING_TYPES =
      EnumSet.of(ExplainTimings.Type.GENERATE_FACET_COUNTS);

  private static final EnumSet<ExplainTimings.Type> HIGHLIGHT_TIMING_TYPES =
      EnumSet.of(ExplainTimings.Type.EXECUTE_HIGHLIGHT, ExplainTimings.Type.SETUP_HIGHLIGHT);

  private static final EnumSet<ExplainTimings.Type> RESULT_MATERIALIZATION_TIMING_TYPES =
      EnumSet.of(ExplainTimings.Type.RETRIEVE_AND_SERIALIZE);

  public QueryExecutionArea(long nanosElapsed, Optional<Map<String, Long>> invocationCounts) {
    // It would be more idiomatic to use Duration here but that would also discard precision as
    // Duration performs an integer divide (values < 1ms become '0').
    this(nanosElapsed / 1000000.0, invocationCounts);
  }

  public static Optional<QueryExecutionArea> notEmptyAreaForType(
      ExplainTimings.Type type, Map<ExplainTimings.Type, TimingData> timings) {

    TimingData timingData = timings.get(type);
    if (TimingData.isEmpty(timingData)) {
      return Optional.empty();
    }

    return Optional.of(queryExecutionAreaFor(EnumSet.of(type), Map.of(type, timingData)));
  }

  static QueryExecutionArea contextAreaFor(Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(CONTEXT_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea matchAreaFor(Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(MATCH_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea sortPrunedResultIterAreaFor(
      Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(
        EnumSet.of(ExplainTimings.Type.NEXT_DOC, ExplainTimings.Type.ADVANCE), timings);
  }

  public static QueryExecutionArea highlightAreaFor(Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(HIGHLIGHT_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea resultMaterializationAreaFor(
      Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(RESULT_MATERIALIZATION_TIMING_TYPES, timings);
  }

  static QueryExecutionArea scoreAreaFor(Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(SCORE_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea collectAreaFor(Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(COLLECT_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea sortComparatorAreaFor(
      Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(SORT_COMPARATOR_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea facetCreateCountAreaFor(
      Map<ExplainTimings.Type, TimingData> timings) {
    return queryExecutionAreaFor(FACET_CREATE_COUNT_TIMING_TYPES, timings);
  }

  public static QueryExecutionArea fromBson(DocumentParser parser) throws BsonParseException {
    return new QueryExecutionArea(
        parser.getField(Fields.MILLIS_ELAPSED).unwrap(),
        parser.getField(Fields.INVOCATION_COUNTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MILLIS_ELAPSED, this.millisElapsed)
        .field(Fields.INVOCATION_COUNTS, this.invocationCounts)
        .build();
  }

  private static QueryExecutionArea queryExecutionAreaFor(
      EnumSet<ExplainTimings.Type> types, Map<ExplainTimings.Type, TimingData> timingData) {
    long elapsedNanos = types.stream().mapToLong(type -> timingData.get(type).elapsedNanos()).sum();
    ImmutableMap<String, Long> invocationCount =
        types.stream()
            .filter(type -> timingData.get(type).invocationCount() > 0)
            .collect(
                toImmutableMap(
                    ExplainTimings.Type::getName,
                    type -> timingData.get(type).invocationCount(),
                    Long::sum));
    return new QueryExecutionArea(
        elapsedNanos, invocationCount.isEmpty() ? Optional.empty() : Optional.of(invocationCount));
  }

  @Override
  public int compareTo(QueryExecutionArea other) {
    return Comparator.comparingDouble(QueryExecutionArea::millisElapsed)
        .thenComparing(QueryExecutionArea::invocationCounts, Optionals::mapCompareTo)
        .compare(this, other);
  }
}
