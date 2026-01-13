package com.xgen.mongot.index.lucene.explain.profiler;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

public class ProfileLeafCollector extends FilterLeafCollector {
  private final ExplainTimings explainTimings;

  public ProfileLeafCollector(LeafCollector leafCollector, ExplainTimings explainTimings) {
    super(leafCollector);
    this.explainTimings = explainTimings;
  }

  @Override
  public void collect(int i) throws IOException {
    try (var ignored = this.explainTimings.split(ExplainTimings.Type.COLLECT)) {
      this.in.collect(i);
    }
  }

  @Override
  public DocIdSetIterator competitiveIterator() throws IOException {
    try (var ignored = this.explainTimings.split(ExplainTimings.Type.COMPETITIVE_ITERATOR)) {
      return this.in.competitiveIterator();
    }
  }

  @Override
  public void setScorer(Scorable scorable) throws IOException {
    try (var ignored = this.explainTimings.split(ExplainTimings.Type.SET_SCORER)) {
      this.in.setScorer(scorable);
    }
  }

  @Override
  public void finish() throws IOException {
    this.in.finish();
  }
}
