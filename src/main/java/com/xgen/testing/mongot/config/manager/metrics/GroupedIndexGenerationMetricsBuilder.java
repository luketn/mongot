package com.xgen.testing.mongot.config.manager.metrics;

import com.xgen.mongot.config.manager.metrics.GroupedIndexGenerationMetrics;
import com.xgen.mongot.config.manager.metrics.IndexGenerationStateMetrics;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;

public class GroupedIndexGenerationMetricsBuilder {
  private Optional<ObjectId> indexId;
  private List<IndexGenerationStateMetrics> indexGenerationStateMetrics = new ArrayList<>();

  public static GroupedIndexGenerationMetricsBuilder builder() {
    return new GroupedIndexGenerationMetricsBuilder();
  }

  public GroupedIndexGenerationMetricsBuilder indexId(ObjectId indexId) {
    this.indexId = Optional.of(indexId);
    return this;
  }

  public GroupedIndexGenerationMetricsBuilder indexGenerationStateMetrics(
      List<IndexGenerationStateMetrics> indexGenerationStateMetrics) {
    this.indexGenerationStateMetrics = indexGenerationStateMetrics;
    return this;
  }

  public GroupedIndexGenerationMetrics build() {
    Check.isPresent(this.indexId, "indexId");
    Check.argNotEmpty(this.indexGenerationStateMetrics, "indexGenerationStateMetrics");

    return new GroupedIndexGenerationMetrics(this.indexId.get(), this.indexGenerationStateMetrics);
  }
}
