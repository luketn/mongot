package com.xgen.mongot.index.lucene.explain.tracing;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.util.Check;

/**
 * A class that provides a builder pattern API over FeatureExplainer's and enforces the correct
 * order of method execution for FeatureExplainers in order to emit explanations.
 */
public class FeatureExplainerEmitter {
  private final FeatureExplainer featureExplainer;
  @Var private boolean aggregateRan;

  FeatureExplainerEmitter(FeatureExplainer featureExplainer) {
    this.featureExplainer = featureExplainer;
    this.aggregateRan = false;
  }

  public static FeatureExplainerEmitter create(FeatureExplainer featureExplainer) {
    return new FeatureExplainerEmitter(featureExplainer);
  }

  public FeatureExplainerEmitter aggregate() {
    this.aggregateRan = true;
    this.featureExplainer.aggregate();
    return this;
  }

  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    Check.checkState(this.aggregateRan, "FeatureExplainer::aggregate must be run before");
    this.featureExplainer.emitExplanation(verbosity, builder);
  }
}
