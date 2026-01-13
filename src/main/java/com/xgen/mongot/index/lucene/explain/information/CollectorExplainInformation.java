package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record CollectorExplainInformation(
    Optional<QueryExecutionArea> allCollectorStats,
    Optional<FacetStats> facetStats,
    Optional<SortStats> sortStats)
    implements DocumentEncodable,
        EqualsWithTimingEquator<CollectorExplainInformation>,
        Comparable<CollectorExplainInformation> {
  static class Fields {
    // TODO(CLOUDP-262345): Link to readme detailing what these sections mean when adding
    // documentation
    static final Field.Optional<QueryExecutionArea> ALL_COLLECTOR_STATS =
        Field.builder("allCollectorStats")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<SortStats> SORT =
        Field.builder("sort")
            .classField(SortStats::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<FacetStats> FACET_STATS =
        Field.builder("facet")
            .classField(FacetStats::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static Optional<CollectorExplainInformation> create(
      Optional<QueryExecutionArea> allCollectorStats,
      Optional<QueryExecutionArea> facetCollectorStats,
      Optional<QueryExecutionArea> createFacetCountStats,
      Optional<Map<String, Integer>> queriedStringFacetCardinalities,
      Optional<Map<String, Integer>> totalStringFacetCardinalities,
      Optional<SortStats> sortStats) {

    Optional<FacetStats> facetStats =
        Stream.of(
                facetCollectorStats,
                createFacetCountStats,
                queriedStringFacetCardinalities,
                totalStringFacetCardinalities)
            .filter(Optional::isPresent)
            .findFirst()
            .map(
                unused ->
                    FacetStats.create(
                        facetCollectorStats,
                        createFacetCountStats,
                        queriedStringFacetCardinalities,
                        totalStringFacetCardinalities));

    if (sortStats.isEmpty() && allCollectorStats.isEmpty() && facetStats.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new CollectorExplainInformation(allCollectorStats, facetStats, sortStats));
  }

  public static CollectorExplainInformation fromBson(DocumentParser parser)
      throws BsonParseException {
    return new CollectorExplainInformation(
        parser.getField(Fields.ALL_COLLECTOR_STATS).unwrap(),
        parser.getField(Fields.FACET_STATS).unwrap(),
        parser.getField(Fields.SORT).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ALL_COLLECTOR_STATS, this.allCollectorStats)
        .field(Fields.FACET_STATS, this.facetStats)
        .field(Fields.SORT, this.sortStats)
        .build();
  }

  @Override
  public boolean equals(
      CollectorExplainInformation other, Equator<QueryExecutionArea> timingEquator) {
    if (this.sortStats.isPresent() != other.sortStats.isPresent()) {
      return false;
    }

    if (this.facetStats.isPresent() != other.facetStats.isPresent()) {
      return false;
    }

    return Equality.equals(this.allCollectorStats, other.allCollectorStats, timingEquator)
        // both sortStats are either present or absent now
        && this.sortStats
            .flatMap(
                stats -> other.sortStats.map(otherStats -> stats.equals(otherStats, timingEquator)))
            .orElse(true)
        // both facetStats are either present or absent now
        && this.facetStats
            .flatMap(
                stats ->
                    other.facetStats.map(otherStats -> stats.equals(otherStats, timingEquator)))
            .orElse(true);
  }

  @Override
  public int compareTo(CollectorExplainInformation other) {
    int allCollectorStatsComparison =
        Optionals.compareTo(this.allCollectorStats, other.allCollectorStats);
    if (allCollectorStatsComparison != 0) {
      return allCollectorStatsComparison;
    }

    int facetStatsComparison = Optionals.compareTo(this.facetStats, other.facetStats);
    if (facetStatsComparison != 0) {
      return facetStatsComparison;
    }

    return Optionals.compareTo(this.sortStats, other.sortStats);
  }
}
