package com.xgen.testing.mongot.index.query.collectors;

import com.xgen.mongot.index.query.collectors.Collector;

public abstract class CollectorBuilder {

  public static FacetCollectorBuilder facet() {
    return new FacetCollectorBuilder();
  }

  public abstract Collector build();
}
