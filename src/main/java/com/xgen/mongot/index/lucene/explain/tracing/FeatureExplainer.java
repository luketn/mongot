package com.xgen.mongot.index.lucene.explain.tracing;

import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;

/**
 * FeatureExplainer objects are used to collect state related to explaining some particular feature
 * and serialize it to the explain response.
 *
 * <p>Note that when adding or updating state that methods may be called concurrently from multiple
 * threads for some requests, so implementations ought to be thread safe.
 */
public interface FeatureExplainer {
  /**
   * Aggregate any per-batch statistics-tracking objects created by a FeatureExplainer into a single
   * form before <code>FeatureExplainer::emitExplanation</code>. Only override the default behavior
   * if the implementer is tracking statistics per batch and compaction is required.
   *
   * <p>This is called shortly before the explain response is serialized so there should not be
   * concurrent access. This method is <b>required</b> to be idempotent as there is no restriction
   * on the number of times Explain.collect() can be called.
   *
   * <p>Note: Must be invoked <i>before</i> <code>FeatureExplainer::emitExplanation</code>.
   */
  default void aggregate() {}

  /**
   * Emit explain state for this feature to the builder. This is called shortly before the response
   * is serialized so there should not be concurrent access.This method is <b>required</b> to be
   * idempotent as there is no restriction on the number of times Explain.collect() can be called.
   *
   * <p>Note: Must be invoked <i>after</i> <code>FeatureExplainer::aggregate</code>.
   *
   * @param verbosity explain verbosity the request is running at.
   * @param builder explanation message builder to update.
   */
  void emitExplanation(Explain.Verbosity verbosity, SearchExplainInformationBuilder builder);
}
