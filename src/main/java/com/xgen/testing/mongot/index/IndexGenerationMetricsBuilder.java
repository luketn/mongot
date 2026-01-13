package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.IndexGenerationMetrics;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class IndexGenerationMetricsBuilder {
  private Optional<GenerationId> generationId;
  private Optional<IndexMetrics> indexMetrics;

  public static IndexGenerationMetricsBuilder builder() {
    return new IndexGenerationMetricsBuilder();
  }

  public IndexGenerationMetricsBuilder generationId(GenerationId generationId) {
    this.generationId = Optional.of(generationId);
    return this;
  }

  public IndexGenerationMetricsBuilder indexMetrics(IndexMetrics indexMetrics) {
    this.indexMetrics = Optional.of(indexMetrics);
    return this;
  }

  public IndexGenerationMetrics build() {
    Check.isPresent(this.generationId, "generationId");
    Check.isPresent(this.indexMetrics, "indexMetrics");

    return new IndexGenerationMetrics(this.generationId.get(), this.indexMetrics.get());
  }
}
