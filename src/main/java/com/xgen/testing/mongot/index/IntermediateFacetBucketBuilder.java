package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonValue;

public class IntermediateFacetBucketBuilder {
  private Optional<IntermediateFacetBucket.Type> type = Optional.empty();
  private Optional<BsonValue> bucket = Optional.empty();
  private Optional<Long> count = Optional.empty();
  private Optional<String> tag = Optional.empty();

  public static IntermediateFacetBucketBuilder builder() {
    return new IntermediateFacetBucketBuilder();
  }

  public IntermediateFacetBucketBuilder type(IntermediateFacetBucket.Type type) {
    this.type = Optional.of(type);
    return this;
  }

  public IntermediateFacetBucketBuilder bucket(BsonValue bucket) {
    this.bucket = Optional.of(bucket);
    return this;
  }

  public IntermediateFacetBucketBuilder count(Long count) {
    this.count = Optional.of(count);
    return this;
  }

  public IntermediateFacetBucketBuilder tag(String tag) {
    this.tag = Optional.of(tag);
    return this;
  }

  /** Builds IntermediateFacetBucket from an IntermediateFacetBucketBuilder. */
  public IntermediateFacetBucket build() {
    Check.isPresent(this.type, "type");
    Check.isPresent(this.tag, "tag");
    Check.isPresent(this.bucket, "bucket");
    Check.isPresent(this.count, "count");

    return new IntermediateFacetBucket(
        this.type.get(), this.tag.get(), this.bucket.get(), this.count.get());
  }
}
