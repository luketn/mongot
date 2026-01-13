package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.FacetBucket;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonValue;

public class FacetBucketBuilder {
  private Optional<BsonValue> id = Optional.empty();
  private Optional<Long> count = Optional.empty();

  public static FacetBucketBuilder builder() {
    return new FacetBucketBuilder();
  }

  public FacetBucketBuilder id(BsonValue id) {
    this.id = Optional.of(id);
    return this;
  }

  public FacetBucketBuilder count(Long count) {
    this.count = Optional.of(count);
    return this;
  }

  /** Builds FacetBucket from an FacetBucketBuilder. */
  public FacetBucket build() {
    Check.isPresent(this.id, "id");
    Check.isPresent(this.count, "count");

    return new FacetBucket(this.id.get(), this.count.get());
  }
}
