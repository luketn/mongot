package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.CollectorExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.FacetStats;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import java.util.Optional;

public class CollectStatsBuilder {
  private Optional<QueryExecutionArea> allCollectorStats = Optional.empty();
  private Optional<SortStats> sortStats = Optional.empty();
  private Optional<FacetStats> facetStats = Optional.empty();

  public static CollectStatsBuilder builder() {
    return new CollectStatsBuilder();
  }

  public CollectStatsBuilder allCollectorStats(QueryExecutionArea allCollectorStats) {
    this.allCollectorStats = Optional.of(allCollectorStats);
    return this;
  }

  public CollectStatsBuilder sortStats(SortStats sortStats) {
    this.sortStats = Optional.of(sortStats);
    return this;
  }

  public CollectStatsBuilder facetStats(FacetStats facetStats) {
    this.facetStats = Optional.of(facetStats);
    return this;
  }

  public CollectorExplainInformation build() {
    return new CollectorExplainInformation(this.allCollectorStats, this.facetStats, this.sortStats);
  }
}
