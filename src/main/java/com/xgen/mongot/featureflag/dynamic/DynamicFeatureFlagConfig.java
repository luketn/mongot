package com.xgen.mongot.featureflag.dynamic;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * Represents the configuration for a dynamic feature flag. This record holds all the rules that
 * determine whether a feature is enabled for a given context.
 *
 * @param featureFlagName The unique name of the feature flag. Existence of featureFlagName is
 *     ensured by validation script.
 * @param phase The operational phase of the flag (e.g., ENABLED, DISABLED, CONTROLLED).
 * @param allowedList A list of {@link ObjectId}s that are explicitly granted access to the feature.
 *     Types and treatments of these IDs is determined by the {@code scope}. For example, if the
 *     scope is {@code USER}, this list contains user IDs.
 * @param blockedList A list of {@link ObjectId}s that are explicitly denied access to the feature.
 *     This list takes precedence over the {@code allowedList} and {@code rolloutPercentage}. Types
 *     and treatments of these IDs is determined by the {@code scope}.
 * @param rolloutPercentage An integer representing the percentage of entities for which the feature
 *     is enabled. While any integer can be provided, the value is internally clamped to the range
 *     [0, 100] by the {@code sanitizedRolloutPercentage()} method. This is typically used when the
 *     {@code phase} is {@code CONTROLLED} for entities not in the allowed or blocked lists.
 * @param scope Defines the context for the {@code allowedList} and {@code blockedList}, indicating
 *     what the {@link ObjectId}s represent (e.g., USER, GROUP, APPLICATION).
 */
public record DynamicFeatureFlagConfig(
    String featureFlagName,
    Phase phase,
    List<ObjectId> allowedList,
    List<ObjectId> blockedList,
    Integer rolloutPercentage,
    Scope scope)
    implements DocumentEncodable {

  /** Compares DynamicFeatureFlagConfigs by their feature flag names. */
  public static final Comparator<DynamicFeatureFlagConfig> FEATURE_FLAG_SORTER =
      Comparator.comparing(DynamicFeatureFlagConfig::featureFlagName, String::compareTo);

  /**
   * Returns the rollout percentage, sanitized to be within the valid range of [0, 100].
   *
   * <p>This method clamps the configured {@code rolloutPercentage} to ensure it is not less than 0
   * or greater than 100.
   *
   * @return an integer between 0 and 100, inclusive.
   */
  public int sanitizedRolloutPercentage() {
    return Math.clamp(this.rolloutPercentage, 0, 100);
  }

  public static class Fields {
    static final Field.Required<String> FEATURE_FLAG_NAME =
        Field.builder("featureFlagName").stringField().required();

    static final Field.WithDefault<DynamicFeatureFlagConfig.Phase> PHASE =
        Field.builder("phase")
            .enumField(DynamicFeatureFlagConfig.Phase.class)
            .withFallback(Phase.UNSPECIFIED)
            .asCaseInsensitive()
            .optional()
            .withDefault(Phase.UNSPECIFIED);

    static final Field.WithDefault<List<ObjectId>> ALLOWED_LIST =
        Field.builder("allowedList").objectIdField().asList().optional().withDefault(List.of());

    static final Field.WithDefault<List<ObjectId>> BLOCKED_LIST =
        Field.builder("blockedList").objectIdField().asList().optional().withDefault(List.of());

    static final Field.WithDefault<Integer> ROLLOUT_PERCENTAGE =
        Field.builder("rolloutPercentage").intField().optional().withDefault(0);

    static final Field.WithDefault<DynamicFeatureFlagConfig.Scope> SCOPE =
        Field.builder("scope")
            .enumField(DynamicFeatureFlagConfig.Scope.class)
            .withFallback(Scope.UNSPECIFIED)
            .asCaseInsensitive()
            .optional()
            .withDefault(Scope.UNSPECIFIED);
  }

  public enum Phase {
    ENABLED, // feature flag always on
    DISABLED, // feature flag always off
    CONTROLLED, // feature flag being evaluated according to rollout percentage
    UNSPECIFIED // unknown state
  }

  public enum Scope {
    UNSPECIFIED, // unknown state
    GROUP, // evaluate at group level via groupId in mmsConfig. ObjectId in allowed list or denied
    // list will be treated as groupId if Scope is specified as GROUP.
    ORG, // evaluate at org level via orgId in mmsConfig. ObjectId in allowed list or denied list
    // will be treated as orgId if Scope is specified as ORG.
    MONGOT_CLUSTER, // evaluate at cluster level. ObjectId in allowed list or denied list will be
    // treated as cluster uniqueId if Scope is specified as MONGOT_CLUSTER.
    MONGOT_QUERY, // evaluate at query level. ObjectId in allowed list or denied list will be
    // ignored if Scope is specified as MONGOT_QUERY.
    MONGOT_INDEX // evaluate at index level. ObjectId will be treated as index id.
  }

  public static DynamicFeatureFlagConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new DynamicFeatureFlagConfig(
        parser.getField(Fields.FEATURE_FLAG_NAME).unwrap(),
        parser.getField(Fields.PHASE).unwrap(),
        parser.getField(Fields.ALLOWED_LIST).unwrap(),
        parser.getField(Fields.BLOCKED_LIST).unwrap(),
        parser.getField(Fields.ROLLOUT_PERCENTAGE).unwrap(),
        parser.getField(Fields.SCOPE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FEATURE_FLAG_NAME, this.featureFlagName)
        .field(Fields.PHASE, this.phase)
        .field(Fields.ALLOWED_LIST, this.allowedList)
        .field(Fields.BLOCKED_LIST, this.blockedList)
        .field(Fields.ROLLOUT_PERCENTAGE, this.rolloutPercentage)
        .field(Fields.SCOPE, this.scope)
        .build();
  }
}
