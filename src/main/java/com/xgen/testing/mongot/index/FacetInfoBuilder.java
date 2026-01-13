package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.FacetBucket;
import com.xgen.mongot.index.FacetInfo;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class FacetInfoBuilder {
  private Optional<List<FacetBucket>> buckets = Optional.empty();

  public static FacetInfoBuilder builder() {
    return new FacetInfoBuilder();
  }

  public FacetInfoBuilder buckets(List<FacetBucket> buckets) {
    this.buckets = Optional.of(buckets);
    return this;
  }

  /** Builds FacetInfo from an FacetInfoBuilder. */
  public FacetInfo build() {
    Check.isPresent(this.buckets, "buckets");

    return new FacetInfo(this.buckets.get());
  }
}
