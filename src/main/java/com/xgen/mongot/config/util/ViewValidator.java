package com.xgen.mongot.config.util;

import com.xgen.mongot.index.definition.InvalidViewDefinitionException;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.definition.ViewDefinition.SupportedStage;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiConsumer;
import com.xgen.mongot.util.functionalinterfaces.CheckedConsumer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class ViewValidator {

  private static final Set<String> DISALLOWED_AGGREGATION_VARIABLES =
      Set.of("$$NOW", "$$CLUSTER_TIME", "$$USER_ROLES");

  private static final Set<String> DISALLOWED_AGGREGATION_OPERATORS = Set.of("$rand", "$function");

  private static final String ID_FIELD = "_id";

  /**
   * Executed for each view definition on index creation and in scope of a conf call cycle, if the
   * view has changed.
   *
   * @throws InvalidViewDefinitionException if the view is incompatible with Atlas Search
   */
  public static void validate(ViewDefinition view) throws InvalidViewDefinitionException {

    if (!view.exists()) {
      throw InvalidViewDefinitionException.missingView(view.getName());
    }

    List<BsonDocument> pipeline = Check.isPresent(view.getEffectivePipeline(), "effectivePipeline");

    for (BsonDocument stage : pipeline) {

      if (stage.keySet().size() > 1) {
        throw InvalidViewDefinitionException.incompatiblePipeline(
            "single stage contains multiple keys: " + stage.keySet());
      }

      String stageName = Check.hasSingleElement(stage.keySet(), "stage");
      Optional<SupportedStage> type = SupportedStage.byStageName(stageName);

      if (type.isEmpty()) {
        throw InvalidViewDefinitionException.incompatiblePipeline(
            String.format("%s stage is not supported", stageName));
      }

      switch (type.get()) {
        case ADD_FIELDS:
        case SET:
          validateAddFields(stage.getDocument(stageName));
          break;
        case MATCH:
          validateMatch(stage.getDocument(stageName));
      }
    }
  }

  private static void validateMatch(BsonDocument match) throws InvalidViewDefinitionException {

    if (match.isEmpty()) {
      // empty match is just a no-op, similar to how mongod handles it
      return;
    }

    if (!match.containsKey("$expr") || match.keySet().size() > 1) {
      throw InvalidViewDefinitionException.incompatiblePipeline(
          "$match stage may only contain $expr, e.g. $match: { $expr: { <expression> } }");
    }

    AggregationPipelineValidator.validateDocument(
        match,
        (field, value) -> {
          checkDisallowedOperatorsByName(field);
          checkAggregationVariablesOverride(field, value);
          validateDisallowedVariablesAreNotUsed(value);
        },
        ViewValidator::validateDisallowedVariablesAreNotUsed);
  }

  private static void validateAddFields(BsonDocument addFields)
      throws InvalidViewDefinitionException {

    if (addFields.containsKey(ID_FIELD)) {
      throw InvalidViewDefinitionException.incompatiblePipeline(
          "modifying _id field is not allowed");
    }

    AggregationPipelineValidator.validateDocument(
        addFields,
        (field, value) -> {
          checkDisallowedOperatorsByName(field);
          checkAggregationVariablesOverride(field, value);
          validateDisallowedVariablesAreNotUsed(value);
        },
        ViewValidator::validateDisallowedVariablesAreNotUsed);
  }

  private static void checkDisallowedOperatorsByName(String field)
      throws InvalidViewDefinitionException {
    if (DISALLOWED_AGGREGATION_OPERATORS.contains(field)) {
      throw InvalidViewDefinitionException.incompatiblePipeline(field + " is not allowed");
    }
  }

  private static void checkAggregationVariablesOverride(String field, BsonValue value)
      throws InvalidViewDefinitionException {
    /*
     * {@link ViewPipeline} overrides the CURRENT variable when the view-defined query gets
     * rewritten into the internal replication query for change streams. We cannot allow users
     * to have the CURRENT variable override in their view pipeline as it won't be compatible
     * when wrapped with our logic.
     */
    if (field.equals("$let") && value.isDocument()) {
      Optional<BsonValue> vars = Optional.ofNullable(value.asDocument().get("vars"));
      if (vars.isPresent()
          && vars.get().isDocument()
          && vars.get().asDocument().containsKey("CURRENT")) {
        throw InvalidViewDefinitionException.incompatiblePipeline(
            "overriding the CURRENT variable is not allowed");
      }
    }
  }

  private static void validateDisallowedVariablesAreNotUsed(BsonValue value)
      throws InvalidViewDefinitionException {

    if (!value.isString()) {
      return;
    }

    String expression = value.asString().getValue();
    boolean containsDisallowedVariable =
        DISALLOWED_AGGREGATION_VARIABLES.stream().anyMatch(expression::contains);

    if (containsDisallowedVariable) {
      throw InvalidViewDefinitionException.incompatiblePipeline(
          "using variables like $$NOW, $$CLUSTER_TIME or $$USER_ROLES is not allowed as "
              + "replication might produce unstable or incorrect results");
    }
  }

  private static class AggregationPipelineValidator {

    /**
     * Recursively visits document's fields and runs provided checks.
     *
     * @throws InvalidViewDefinitionException if a check fails
     */
    static void validateDocument(
        BsonDocument document,
        CheckedBiConsumer<String, BsonValue, InvalidViewDefinitionException> documentChecks,
        CheckedConsumer<BsonValue, InvalidViewDefinitionException> arrayChecks)
        throws InvalidViewDefinitionException {
      for (var entry : document.entrySet()) {

        if (entry.getValue().isDocument()) {
          validateDocument(entry.getValue().asDocument(), documentChecks, arrayChecks);
        }

        if (entry.getValue().isArray()) {
          validateArray(entry.getValue().asArray(), documentChecks, arrayChecks);
        }

        documentChecks.accept(entry.getKey(), entry.getValue());
      }
    }

    private static void validateArray(
        BsonArray array,
        CheckedBiConsumer<String, BsonValue, InvalidViewDefinitionException> documentChecks,
        CheckedConsumer<BsonValue, InvalidViewDefinitionException> arrayChecks)
        throws InvalidViewDefinitionException {
      for (var element : array) {
        if (element.isDocument()) {
          validateDocument(element.asDocument(), documentChecks, arrayChecks);
        }
        if (element.isArray()) {
          validateArray(element.asArray(), documentChecks, arrayChecks);
        }
        arrayChecks.accept(element);
      }
    }
  }
}
