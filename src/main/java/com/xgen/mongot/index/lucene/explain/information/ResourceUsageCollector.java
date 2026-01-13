package com.xgen.mongot.index.lucene.explain.information;

import com.google.errorprone.annotations.Immutable;

@Immutable
public record ResourceUsageCollector(
    long majorFaults, long minorFaults, long userTimeMs, long systemTimeMs, int reportingThreads) {

  public static final ResourceUsageCollector EMPTY = new ResourceUsageCollector(0, 0, 0, 0, 0);

  public static ResourceUsageCollector sum(ResourceUsageCollector a, ResourceUsageCollector b) {
    return new ResourceUsageCollector(
        a.majorFaults + b.majorFaults,
        a.minorFaults + b.minorFaults,
        a.userTimeMs + b.userTimeMs,
        a.systemTimeMs + b.systemTimeMs,
        a.reportingThreads + b.reportingThreads);
  }
}
