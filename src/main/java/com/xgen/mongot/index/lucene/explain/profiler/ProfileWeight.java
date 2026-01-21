package com.xgen.mongot.index.lucene.explain.profiler;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

public class ProfileWeight extends Weight {

  private final Weight subQueryWeight;
  private final ExplainTimings timings;

  public ProfileWeight(Query query, Weight subQueryWeight, ExplainTimings timings) {
    super(query);
    this.subQueryWeight = subQueryWeight;
    this.timings = timings;
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    return this.subQueryWeight.explain(context, doc);
  }

  /** A scorer for ProfileWeight. May return null. */
  @Override
  @Nullable
  public Scorer scorer(LeafReaderContext context) throws IOException {
    Optional<ScorerSupplier> supplier = optionalScorerSupplier(context);
    if (supplier.isEmpty()) {
      return null;
    }
    return supplier.get().get(Long.MAX_VALUE);
  }

  /** Get a ScorerSupplier. May be null. */
  @Override
  @Nullable
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    Check.argNotNull(context, "context");
    return optionalScorerSupplier(context).orElse(null);
  }

  /** Get a ScorerSupplier, but return an empty optional (instead of null). */
  private Optional<ScorerSupplier> optionalScorerSupplier(LeafReaderContext context)
      throws IOException {
    Check.argNotNull(context, "context");
    Optional<ScorerSupplier> maybeSubqueryScorerSupplier;
    try (var ignored = this.timings.split(ExplainTimings.Type.CREATE_SCORER)) {
      try {
        maybeSubqueryScorerSupplier =
            Optional.ofNullable(this.subQueryWeight.scorerSupplier(context));
      } catch (UnsupportedOperationException e) {
        // Some query types (e.g., DrillSidewaysQuery) don't support scorerSupplier()
        return Optional.empty();
      }
    }

    if (maybeSubqueryScorerSupplier.isEmpty()) {
      return Optional.empty();
    }

    ProfileWeight weight = this;
    ScorerSupplier subqueryScorerSupplier = maybeSubqueryScorerSupplier.get();

    return Optional.of(
        new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            try (var ignored = weight.timings.split(ExplainTimings.Type.CREATE_SCORER)) {
              return new ProfileScorer(
                  weight, subqueryScorerSupplier.get(leadCost), weight.timings);
            }
          }

          @Override
          public long cost() {
            try (var ignored = weight.timings.split(ExplainTimings.Type.CREATE_SCORER)) {
              return subqueryScorerSupplier.cost();
            }
          }
        });
  }

  @Override
  @Nullable
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    var bulkScorer = super.bulkScorer(context);
    if (bulkScorer != null) {
      return bulkScorer;
    }

    // Lucene's DrillSidewaysQuery does not implement scorer()/scorerSupplier(), so
    // Weight#bulkScorer() returns null. In this case, we must explicitly
    // delegate to the subQueryWeight's bulkScorer for scoring to proceed.
    if (this.parentQuery
        .getClass()
        .getName()
        .equals("org.apache.lucene.facet.DrillSidewaysQuery")) {
      return this.subQueryWeight.bulkScorer(context);
    }
    return null;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    // Each ProfileWeight will hold the timing information associated with the running explain
    // query - we don't want to use the cached timing statistics from a past explain call.
    return false;
  }
}
