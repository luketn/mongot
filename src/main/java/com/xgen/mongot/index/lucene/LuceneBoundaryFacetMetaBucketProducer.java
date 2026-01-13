package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.facet.FacetResult;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * LuceneBoundaryFacetMetaBucketProducer is responsible for formatting buckets for a single {@link
 * FacetDefinition.BoundaryFacetDefinition} facet. It uses an array of {@link
 * org.apache.lucene.facet.LabelAndValue} that holds the facet bucket labels and their corresponding
 * counts to build facet buckets. nextBucket() should be called to output buckets for each of the
 * label/value pairs until isExhausted() is true. If the facetDefinition requests a default bucket,
 * this will be computed and output last, by subtracting the count of documents in all specified
 * facet buckets from the total count of documents reported in the search.
 */
class LuceneBoundaryFacetMetaBucketProducer implements LuceneMetaBucketProducer {

  private final List<IntermediateFacetBucket> buckets;
  private final Optional<IntermediateFacetBucket> defaultBucket;

  private int position;

  public LuceneBoundaryFacetMetaBucketProducer(
      List<IntermediateFacetBucket> buckets, Optional<IntermediateFacetBucket> defaultBucket) {
    this.buckets = buckets;
    this.defaultBucket = defaultBucket;

    this.position = 0;
  }

  public static LuceneBoundaryFacetMetaBucketProducer create(
      FacetResult facetResult,
      String facetName,
      FacetDefinition.BoundaryFacetDefinition<? extends BsonValue> facetDefinition,
      long totalCount) {
    @Var long facetDocsCount = 0;
    List<IntermediateFacetBucket> buckets = new ArrayList<>();
    for (int i = 0; i < facetResult.labelValues.length; i++) {
      long bucketCount = facetResult.labelValues[i].value.longValue();
      facetDocsCount += bucketCount;
      buckets.add(
          new IntermediateFacetBucket(
              IntermediateFacetBucket.Type.FACET,
              facetName,
              facetDefinition.boundaries().get(i),
              bucketCount));
    }

    Optional<IntermediateFacetBucket> defaultBucket =
        facetDefinition.defaultBucketName().isPresent()
            ? Optional.of(
                new IntermediateFacetBucket(
                    IntermediateFacetBucket.Type.FACET,
                    facetName,
                    new BsonString(facetDefinition.defaultBucketName().get()),
                    totalCount - facetDocsCount))
            : Optional.empty();

    return new LuceneBoundaryFacetMetaBucketProducer(buckets, defaultBucket);
  }

  @Override
  public IntermediateFacetBucket peek() {
    checkState(!isExhausted(), "Can't call peek() when BucketProducer is exhausted");

    if (this.position < this.buckets.size()) {
      return this.buckets.get(this.position);
    }

    if (this.position == this.buckets.size()) {
      // We have checked that we are not exhausted. We must have a default bucket to produce.
      checkState(this.defaultBucket.isPresent(), "defaultBucket is missing");
      return this.defaultBucket.get();
    }

    return Check.unreachable("Position is neither within buckets nor at default bucket");
  }

  @Override
  public void acceptAndAdvance() {
    checkState(!isExhausted(), "Can't call accept() when BucketProducer is exhausted");

    this.position++;
  }

  @Override
  public boolean isExhausted() {
    return this.position > this.buckets.size()
        || (this.position == this.buckets.size() && this.defaultBucket.isEmpty());
  }
}
