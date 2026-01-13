package com.xgen.testing.mongot.index.lucene.explain.timing;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimingBreakdown;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ExplainTimingBreakdownBuilder {
  private Optional<QueryExecutionArea> context;
  private Optional<QueryExecutionArea> match;
  private Optional<QueryExecutionArea> score;

  public static ExplainTimingBreakdownBuilder builder() {
    return new ExplainTimingBreakdownBuilder();
  }

  public ExplainTimingBreakdownBuilder context(QueryExecutionArea context) {
    this.context = Optional.of(context);
    return this;
  }

  public ExplainTimingBreakdownBuilder match(QueryExecutionArea match) {
    this.match = Optional.of(match);
    return this;
  }

  public ExplainTimingBreakdownBuilder score(QueryExecutionArea score) {
    this.score = Optional.of(score);
    return this;
  }

  /** Builds an ExplainTimingBreakdown from a ExplainTimingBreakdownBuilder. */
  public ExplainTimingBreakdown build() {
    Check.isPresent(this.context, "context");
    Check.isPresent(this.match, "match");
    Check.isPresent(this.score, "score");

    return new ExplainTimingBreakdown(this.context.get(), this.match.get(), this.score.get());
  }
}
