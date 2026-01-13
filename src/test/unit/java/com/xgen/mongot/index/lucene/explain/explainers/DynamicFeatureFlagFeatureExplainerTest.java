package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.lucene.explain.information.FeatureFlagEvaluationSpec;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class DynamicFeatureFlagFeatureExplainerTest {
  @Test
  public void evaluate_missingFeatureFlag_returnsFallbackValue() throws Exception {
    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION), Optional.empty())) {
      DynamicFeatureFlagRegistry flags =
          new DynamicFeatureFlagRegistry(
              Optional.of(List.of()), Optional.empty(), Optional.empty(), Optional.empty());
      flags.evaluate("test", new ObjectId(), false);

      var actual = Explain.collect().get().dynamicFeatureFlags().get();
      var expectedDffs =
          List.of(
              new FeatureFlagEvaluationSpec(
                  "test", false, FeatureFlagEvaluationSpec.DecisiveField.FALLBACK));
      Assert.assertEquals(expectedDffs, actual);
    }
  }

  @Test
  public void evaluate_phaseEnabled_returnsTrue() throws Exception {
    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION), Optional.empty())) {
      DynamicFeatureFlagRegistry flags =
          new DynamicFeatureFlagRegistry(
              Optional.of(
                  List.of(
                      new DynamicFeatureFlagConfig(
                          "test",
                          DynamicFeatureFlagConfig.Phase.ENABLED,
                          List.of(),
                          List.of(),
                          0,
                          DynamicFeatureFlagConfig.Scope.ORG))),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      flags.evaluate("test", new ObjectId(), false);

      var actual = Explain.collect().get().dynamicFeatureFlags().get();
      var expectedDffs =
          List.of(
              new FeatureFlagEvaluationSpec(
                  "test", true, FeatureFlagEvaluationSpec.DecisiveField.PHASE));
      Assert.assertEquals(expectedDffs, actual);
    }
  }

  @Test
  public void evaluate_idInAllowList_returnsTrue() throws Exception {
    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION), Optional.empty())) {
      ObjectId allowedId = new ObjectId();
      DynamicFeatureFlagRegistry flags =
          new DynamicFeatureFlagRegistry(
              Optional.of(
                  List.of(
                      new DynamicFeatureFlagConfig(
                          "test",
                          DynamicFeatureFlagConfig.Phase.CONTROLLED,
                          List.of(allowedId),
                          List.of(),
                          0,
                          DynamicFeatureFlagConfig.Scope.ORG))),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      flags.evaluate("test", allowedId, false);

      var actual = Explain.collect().get().dynamicFeatureFlags().get();
      var expectedDffs =
          List.of(
              new FeatureFlagEvaluationSpec(
                  "test", true, FeatureFlagEvaluationSpec.DecisiveField.ALLOW_LIST));
      Assert.assertEquals(expectedDffs, actual);
    }
  }

  @Test
  public void evaluate_idInBlockList_returnsFalse() throws Exception {
    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION), Optional.empty())) {
      ObjectId blockedId = new ObjectId();
      DynamicFeatureFlagRegistry flags =
          new DynamicFeatureFlagRegistry(
              Optional.of(
                  List.of(
                      new DynamicFeatureFlagConfig(
                          "test",
                          DynamicFeatureFlagConfig.Phase.CONTROLLED,
                          List.of(),
                          List.of(blockedId),
                          0,
                          DynamicFeatureFlagConfig.Scope.ORG))),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      flags.evaluate("test", blockedId, false);

      var actual = Explain.collect().get().dynamicFeatureFlags().get();
      var expectedDffs =
          List.of(
              new FeatureFlagEvaluationSpec(
                  "test", false, FeatureFlagEvaluationSpec.DecisiveField.BLOCK_LIST));
      Assert.assertEquals(expectedDffs, actual);
    }
  }

  @Test
  public void evaluate_rolloutPercentageFails_returnsFalse() throws Exception {
    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION), Optional.empty())) {
      ObjectId objId = new ObjectId("656e184b9c1d322199b5a371");
      DynamicFeatureFlagRegistry flags =
          new DynamicFeatureFlagRegistry(
              Optional.of(
                  List.of(
                      new DynamicFeatureFlagConfig(
                          "test",
                          DynamicFeatureFlagConfig.Phase.CONTROLLED,
                          List.of(),
                          List.of(),
                          70,
                          DynamicFeatureFlagConfig.Scope.ORG))),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      flags.evaluate("test", objId, false);

      var actual = Explain.collect().get().dynamicFeatureFlags().get();
      var expectedDffs =
          List.of(
              new FeatureFlagEvaluationSpec(
                  "test", false, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE));
      Assert.assertEquals(expectedDffs, actual);
    }
  }

  @Test
  public void evaluate_rolloutPercentagePasses_returnsTrue() throws Exception {
    try (var unused =
        Explain.setup(Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION), Optional.empty())) {
      ObjectId objId = new ObjectId("655e0c7a8b9d0e1f2a3b4c5d");
      DynamicFeatureFlagRegistry flags =
          new DynamicFeatureFlagRegistry(
              Optional.of(
                  List.of(
                      new DynamicFeatureFlagConfig(
                          "test",
                          DynamicFeatureFlagConfig.Phase.CONTROLLED,
                          List.of(),
                          List.of(),
                          30,
                          DynamicFeatureFlagConfig.Scope.ORG))),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      flags.evaluate("test", objId, false);

      var actual = Explain.collect().get().dynamicFeatureFlags().get();
      var expectedDffs =
          List.of(
              new FeatureFlagEvaluationSpec(
                  "test", true, FeatureFlagEvaluationSpec.DecisiveField.ROLLOUT_PERCENTAGE));
      Assert.assertEquals(expectedDffs, actual);
    }
  }
}
