package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.FeatureFlagEvaluationSpec;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import java.util.ArrayList;
import java.util.List;

public class DynamicFeatureFlagFeatureExplainer implements FeatureExplainer {
  private final List<FeatureFlagEvaluationSpec> featureFlagEvaluationSpecs = new ArrayList<>();

  public void addFeatureFlagEvaluationSpec(FeatureFlagEvaluationSpec featureFlagEvaluationSpec) {
    this.featureFlagEvaluationSpecs.add(featureFlagEvaluationSpec);
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    builder.dynamicFeatureFlags(this.featureFlagEvaluationSpecs);
  }
}
