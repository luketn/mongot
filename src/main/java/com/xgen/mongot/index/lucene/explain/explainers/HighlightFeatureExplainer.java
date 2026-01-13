package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.HighlightStats;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import com.xgen.mongot.index.query.highlights.Highlight;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;

public class HighlightFeatureExplainer implements FeatureExplainer {
  private final ExplainTimings timings;
  private final Highlight highlight;

  /**
   * Mapping from field name to the selected source of term offsets.
   *
   * <p>The offset source is selected based on indexing options and query shapes. This is critical
   * for accurate and performant highlighting.
   */
  private final Map<String, UnifiedHighlighter.OffsetSource> offsetSource = new HashMap<>();

  public HighlightFeatureExplainer(Highlight highlight) {
    this.timings = ExplainTimings.builder().build();
    this.highlight = highlight;
  }

  public ExplainTimings getTimings() {
    return this.timings;
  }

  public void addOffsetSource(String field, UnifiedHighlighter.OffsetSource offsetSource) {
    this.offsetSource.put(field, offsetSource);
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      builder.highlightStats(
          HighlightStats.create(this.highlight, this.offsetSource, Optional.empty()));
      return;
    }

    builder.highlightStats(
        HighlightStats.create(
            this.highlight,
            this.offsetSource,
            Optional.of(QueryExecutionArea.highlightAreaFor(this.timings.extractTimingData()))));
  }
}
