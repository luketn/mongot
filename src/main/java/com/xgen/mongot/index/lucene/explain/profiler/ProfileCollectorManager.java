package com.xgen.mongot.index.lucene.explain.profiler;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * ProfileCollectorManager facilitates profiling any LeafCollectors produced via the supplied
 * CollectorManager. A CollectorManager is only used in the initial <code>IndexSearcher::search
 * </code> and not in <code>IndexSearcher::searchAfter</code> therefore it is not necessary to use a
 * supplier for the ExplainTimings to update Explain state across batches.
 */
public class ProfileCollectorManager<C extends Collector, T>
    implements CollectorManager<ProfileCollectorManager.ProfileCollector<C>, T> {
  CollectorManager<C, T> collectorManager;
  ExplainTimings explainTimings;

  public ProfileCollectorManager(
      CollectorManager<C, T> collectorManager, ExplainTimings explainTimings) {
    this.collectorManager = collectorManager;
    this.explainTimings = explainTimings;
  }

  @Override
  public ProfileCollector<C> newCollector() throws IOException {
    return new ProfileCollector<>(this.collectorManager.newCollector(), this.explainTimings);
  }

  @Override
  public T reduce(Collection<ProfileCollector<C>> collectors) throws IOException {
    return this.collectorManager.reduce(
        collectors.stream().map(ProfileCollector::getCollector).toList());
  }

  public static class ProfileCollector<C extends Collector> implements Collector {
    private final C collector;
    private final ExplainTimings explainTimings;

    ProfileCollector(C collector, ExplainTimings explainTimings) {
      this.collector = collector;
      this.explainTimings = explainTimings;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new ProfileLeafCollector(
          this.collector.getLeafCollector(context), this.explainTimings);
    }

    @Override
    public ScoreMode scoreMode() {
      return this.collector.scoreMode();
    }

    @Override
    public void setWeight(Weight weight) {
      this.collector.setWeight(weight);
    }

    public C getCollector() {
      return this.collector;
    }
  }
}
