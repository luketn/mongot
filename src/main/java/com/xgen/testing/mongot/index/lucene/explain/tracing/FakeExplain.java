package com.xgen.testing.mongot.index.lucene.explain.tracing;

import com.google.errorprone.annotations.MustBeClosed;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/** Inject fake information into the explain system for testing. */
public class FakeExplain {
  private FakeExplain() {}

  public static class FakeQueryState extends ExplainQueryState {
    private final SearchExplainInformation explainInformation;

    public FakeQueryState(
        Explain.Verbosity verbosity,
        int numPartitions,
        SearchExplainInformation explainInformation) {
      super(new Explain.QueryInfo(verbosity), numPartitions);
      this.explainInformation = explainInformation;
    }

    @Override
    public synchronized SearchExplainInformation collect() {
      return this.explainInformation;
    }
  }

  /**
   * Inject fake explain state in the current context that will always collect explainInformation.
   *
   * <p>Further calls to <code>Explain.setup()</code> will not override this state, ensuring that
   * <code>Explain.collect()</code> always returns <code>Optional.of(explainInformation)</code>.
   *
   * @param verbosity explain verbosity
   * @param explainInformation explain result to return on <code>Explain.collect()</code>
   * @return <code>Scope</code> for which this override will exist.
   */
  @MustBeClosed
  public static Scope setup(
      Explain.Verbosity verbosity, int numPartitions, SearchExplainInformation explainInformation) {
    return Context.current()
        .with(new FakeQueryState(verbosity, numPartitions, explainInformation))
        .makeCurrent();
  }
}
