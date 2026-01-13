package com.xgen.mongot.index;

import com.google.errorprone.annotations.MustBeClosed;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the state of dynamic feature flags evaluated during a single request/query. This allows us
 * to tie the final query latency to the specific flags used.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (var unused = DynamicFeatureFlagsMetricsRecorder.setup()) {
 *   // Feature flag evaluations within this scope are recorded
 *   registry.evaluate("my-flag", entityId, false);
 *   // ...
 *   List<Tags> tagsPerFlag = DynamicFeatureFlagsMetricsRecorder.getTagsPerFlag();
 * }
 * }</pre>
 */
public class DynamicFeatureFlagsMetricsRecorder {

  public static final String FEATURE_FLAG_NAME_TAG = "featureFlagName";
  public static final String EVALUATION_RESULT_TAG = "evaluationResult";

  private static final ContextKey<Map<String, Boolean>> EVALUATED_FLAGS_KEY =
      ContextKey.named("mongot.evaluatedDynamicFeatureFlags");

  /**
   * Sets up a new context for recording feature flag evaluations. The returned scope should be used
   * with try-with-resources to ensure proper cleanup.
   *
   * @return A closeable scope that, when closed, restores the previous context
   */
  @MustBeClosed
  public static Scope setup() {
    return Context.current().with(EVALUATED_FLAGS_KEY, new ConcurrentHashMap<>()).makeCurrent();
  }

  /**
   * Called by the Registry when a flag is evaluated. Records the evaluation in the current context
   * if one exists.
   *
   * @param featureFlagName The name of the flag.
   * @param evaluationResult The result of evaluating featureFlagName
   */
  public static void recordEvaluation(String featureFlagName, boolean evaluationResult) {
    getEvaluatedFlagsMap().ifPresent(map -> map.put(featureFlagName, evaluationResult));
  }

  /**
   * Gets the evaluated flags as a list of Micrometer Tags for metrics recording. Each entry in the
   * list contains the tags for a single flag evaluation, with {@code featureFlagName} and {@code
   * evaluationResult} tags.
   *
   * @return List of Tags, each representing a single feature flag evaluation with featureFlagName
   *     and evaluationResult tags, or an empty list if no context exists
   */
  public static List<Tags> getTagsPerFlag() {
    return getEvaluatedFlagsMap()
        .map(
            map ->
                map.entrySet().stream()
                    .map(
                        entry ->
                            Tags.of(
                                FEATURE_FLAG_NAME_TAG,
                                entry.getKey(),
                                EVALUATION_RESULT_TAG,
                                String.valueOf(entry.getValue())))
                    .toList())
        .orElse(List.of());
  }

  private static Optional<Map<String, Boolean>> getEvaluatedFlagsMap() {
    return Optional.ofNullable(Context.current().get(EVALUATED_FLAGS_KEY));
  }
}
