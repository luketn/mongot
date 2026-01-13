package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record FacetStats(
    Optional<QueryExecutionArea> collectorStats,
    Optional<QueryExecutionArea> createCountsStats,
    Optional<Map<String, CardinalityInfo>> stringFacetCardinalityInfo)
    implements DocumentEncodable, Comparable<FacetStats> {
  static class Fields {
    static final Field.Optional<QueryExecutionArea> COLLECTOR_STATS =
        Field.builder("collectorStats")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<QueryExecutionArea> CREATE_COUNTS_STATS =
        Field.builder("createCountsStats")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<Map<String, CardinalityInfo>> STRING_FACET_CARDINALITIES =
        Field.builder("stringFacetCardinalities")
            .mapOf(
                Value.builder()
                    .classValue(CardinalityInfo::fromBson)
                    .disallowUnknownFields()
                    .required())
            .optional()
            .noDefault();
  }

  public record CardinalityInfo(int queried, int total)
      implements Encodable, Comparable<CardinalityInfo> {
    private static class Fields {
      private static final Field.Required<Integer> QUERIED =
          Field.builder("queried").intField().required();

      private static final Field.Required<Integer> TOTAL =
          Field.builder("total").intField().required();
    }

    public static CardinalityInfo fromBson(DocumentParser parser) throws BsonParseException {
      return new CardinalityInfo(
          parser.getField(Fields.QUERIED).unwrap(), parser.getField(Fields.TOTAL).unwrap());
    }

    @Override
    public BsonValue toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.QUERIED, this.queried)
          .field(Fields.TOTAL, this.total)
          .build();
    }

    @Override
    public int compareTo(FacetStats.CardinalityInfo other) {
      return Comparator.comparing(CardinalityInfo::queried)
          .thenComparing(CardinalityInfo::total)
          .compare(this, other);
    }
  }

  static FacetStats create(
      Optional<QueryExecutionArea> collectorStats,
      Optional<QueryExecutionArea> createCountsStats,
      Optional<Map<String, Integer>> queriedStringFacetCardinalities,
      Optional<Map<String, Integer>> totalStringFacetCardinalities) {
    return new FacetStats(
        collectorStats,
        createCountsStats,
        parseCardinalityInfo(queriedStringFacetCardinalities, totalStringFacetCardinalities));
  }

  private static Optional<Map<String, CardinalityInfo>> parseCardinalityInfo(
      Optional<Map<String, Integer>> queriedStringFacetCardinalities,
      Optional<Map<String, Integer>> totalStringFacetCardinalities) {
    if (queriedStringFacetCardinalities.isEmpty() && totalStringFacetCardinalities.isEmpty()) {
      return Optional.empty();
    }

    // If one is present, both should be present
    Map<String, CardinalityInfo> mergedCardinalities =
        queriedStringFacetCardinalities.get().keySet().stream()
            .collect(
                CollectionUtils.toMapUnsafe(
                    key -> key,
                    key ->
                        new CardinalityInfo(
                            queriedStringFacetCardinalities.get().getOrDefault(key, 0),
                            totalStringFacetCardinalities.get().getOrDefault(key, 0))));

    return Optional.of(mergedCardinalities);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COLLECTOR_STATS, this.collectorStats)
        .field(Fields.CREATE_COUNTS_STATS, this.createCountsStats)
        .field(Fields.STRING_FACET_CARDINALITIES, this.stringFacetCardinalityInfo)
        .build();
  }

  public static FacetStats fromBson(DocumentParser parser) throws BsonParseException {
    return new FacetStats(
        parser.getField(Fields.COLLECTOR_STATS).unwrap(),
        parser.getField(Fields.CREATE_COUNTS_STATS).unwrap(),
        parser.getField(Fields.STRING_FACET_CARDINALITIES).unwrap());
  }

  public boolean equals(FacetStats other, Equator<QueryExecutionArea> timingEquator) {
    return Equality.equals(this.collectorStats, other.collectorStats, timingEquator)
        && Equality.equals(this.createCountsStats, other.createCountsStats, timingEquator)
        && Objects.equals(this.stringFacetCardinalityInfo, other.stringFacetCardinalityInfo);
  }

  @Override
  public int compareTo(FacetStats other) {
    int collectorStatsComparison = Optionals.compareTo(this.collectorStats, other.collectorStats);
    if (collectorStatsComparison != 0) {
      return collectorStatsComparison;
    }

    int createCountsStatsComparison =
        Optionals.compareTo(this.createCountsStats, other.createCountsStats);
    if (createCountsStatsComparison != 0) {
      return createCountsStatsComparison;
    }

    return Optionals.mapCompareTo(
        this.stringFacetCardinalityInfo, other.stringFacetCardinalityInfo);
  }
}
