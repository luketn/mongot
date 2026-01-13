package com.xgen.mongot.index.lucene.explain.information;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ResourceUsageCollectorTest {
  @Test
  public void testSum() {
    var usage1 = new ResourceUsageCollector(1, 2, 3, 4, 2);
    var usage2 = new ResourceUsageCollector(1, 2, 3, 4, 1);
    assertEquals(
        new ResourceUsageCollector(2, 4, 6, 8, 3), ResourceUsageCollector.sum(usage1, usage2));
  }
}
