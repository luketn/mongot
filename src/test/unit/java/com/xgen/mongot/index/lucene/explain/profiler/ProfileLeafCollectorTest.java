package com.xgen.mongot.index.lucene.explain.profiler;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.io.IOException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.junit.Assert;
import org.junit.Test;

public class ProfileLeafCollectorTest {
  private static LeafCollector createCollector() {
    return new TopScoreDocCollector.ScorerLeafCollector() {
      @Override
      public void collect(int doc) throws IOException {}
    };
  }

  @Test
  public void testCollectTiming() throws IOException {
    var timings = ExplainTimings.builder().build();
    LeafCollector collector = new ProfileLeafCollector(createCollector(), timings);
    collector.collect(0);
    collector.collect(1);
    collector.collect(2);

    Assert.assertEquals(3, timings.ofType(ExplainTimings.Type.COLLECT).getInvocationCount());
  }

  @Test
  public void testCompetitiveIteratorTiming() throws IOException {
    var timings = ExplainTimings.builder().build();
    LeafCollector collector = new ProfileLeafCollector(createCollector(), timings);
    collector.competitiveIterator();
    collector.competitiveIterator();

    Assert.assertEquals(
        2, timings.ofType(ExplainTimings.Type.COMPETITIVE_ITERATOR).getInvocationCount());
  }
}
