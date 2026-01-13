package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.FacetStats;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import java.util.Map;
import java.util.Optional;

public class FacetStatsBuilder {
  private Optional<QueryExecutionArea> collectorStats = Optional.empty();
  private Optional<QueryExecutionArea> createFacetCountStats = Optional.empty();
  private Optional<Map<String, FacetStats.CardinalityInfo>> stringFacetCardinalityInfo =
      Optional.empty();

  public static FacetStatsBuilder builder() {
    return new FacetStatsBuilder();
  }

  public FacetStatsBuilder collectorStats(QueryExecutionArea collectorStats) {
    this.collectorStats = Optional.of(collectorStats);
    return this;
  }

  public FacetStatsBuilder createFacetCountStats(QueryExecutionArea createFacetCountStats) {
    this.createFacetCountStats = Optional.of(createFacetCountStats);
    return this;
  }

  public FacetStatsBuilder stringFacetCardinalityInfo(
      Map<String, FacetStats.CardinalityInfo> stringFacetCardinalityInfo) {
    this.stringFacetCardinalityInfo = Optional.of(stringFacetCardinalityInfo);
    return this;
  }

  public FacetStats build() {
    return new FacetStats(
        this.collectorStats, this.createFacetCountStats, this.stringFacetCardinalityInfo);
  }
}
