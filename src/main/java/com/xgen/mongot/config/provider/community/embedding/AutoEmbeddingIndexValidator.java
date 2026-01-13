package com.xgen.mongot.config.provider.community.embedding;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Validator for auto-embedding vector indexes on community.
 *
 * <p>These validations are ONLY applied to auto-embedding indexes (indexes that contain TEXT or
 * AUTO_EMBED field types).
 */
public class AutoEmbeddingIndexValidator {

  /**
   * Runs a list of validations for auto-embedding indexes on community
   *
   * <ul>
   *   <li>IndexDefinition can only contain registered models
   *   <li>Single IndexDefinition can only contain one model
   *   <li>IndexDefinition cannot mix regular vector fields with auto-embedding fields
   * </ul>
   *
   * @throws InvalidIndexDefinitionException if the index violates restrictions
   * @throws IllegalArgumentException if called on a non-auto-embedding index
   */
  public static void validate(VectorIndexDefinition vectorIndex)
      throws InvalidIndexDefinitionException {
    if (!vectorIndex.isAutoEmbeddingIndex()) {
      throw new IllegalArgumentException(
          "Unable to invoke validations on non auto-embedding indexes");
    }

    validateEmbeddingModelsAreRegistered(vectorIndex);
    validateSingleEmbeddingModel(vectorIndex);
    validateNoMixedVectorTypes(vectorIndex);
  }

  /**
   * Extracts the embedding model name from a field definition, if applicable.
   *
   * @param field the field definition to extract from
   * @return the model name, or null if the field doesn't have an embedding model
   */
  private static Optional<String> extractModelName(VectorIndexFieldDefinition field) {
    return switch (field) {
      case VectorTextFieldDefinition textField ->
          Optional.of(textField.specification().modelName());
      case VectorAutoEmbedFieldDefinition autoEmbedField ->
          Optional.of(autoEmbedField.specification().modelName());
      default -> Optional.empty();
    };
  }

  /**
   * Collects all embedding model names used in the index.
   *
   * @param vectorIndex the vector index to extract model names from
   * @return a set of all model names found in the index
   */
  private static Set<String> collectModelNames(VectorIndexDefinition vectorIndex) {
    Set<String> modelNames = new HashSet<>();
    for (VectorIndexFieldDefinition field : vectorIndex.getFields()) {
      Optional<String> modelName = extractModelName(field);
      modelName.ifPresent(modelNames::add);
    }
    return modelNames;
  }

  /**
   * Validates that all embedding models referenced in the index are registered in the
   * EmbeddingModelCatalog.
   *
   * @param vectorIndex the vector index to validate
   * @throws InvalidIndexDefinitionException if any referenced model is not registered
   */
  private static void validateEmbeddingModelsAreRegistered(VectorIndexDefinition vectorIndex)
      throws InvalidIndexDefinitionException {
    Set<String> modelNames = collectModelNames(vectorIndex);
    List<String> unregisteredModels = new ArrayList<>();

    for (String modelName : modelNames) {
      if (!EmbeddingModelCatalog.isModelRegistered(modelName)) {
        unregisteredModels.add(modelName);
      }
    }

    if (!unregisteredModels.isEmpty()) {
      Set<String> registeredModels = EmbeddingModelCatalog.getAllSupportedModels();
      String supportedModelsList =
          registeredModels.isEmpty()
              ? "none (embedding service not configured)"
              : String.join(", ", registeredModels);
      throw new InvalidIndexDefinitionException(
          String.format(
              "The following embedding model(s) are not supported: %s. " + "Supported models: %s.",
              String.join(", ", unregisteredModels), supportedModelsList));
    }
  }

  /**
   * Validates that the index uses only a single embedding model.
   *
   * @param vectorIndex the vector index to validate
   * @throws InvalidIndexDefinitionException if multiple embedding models are used
   */
  private static void validateSingleEmbeddingModel(VectorIndexDefinition vectorIndex)
      throws InvalidIndexDefinitionException {
    Set<String> modelNames = collectModelNames(vectorIndex);

    if (modelNames.size() > 1) {
      throw new InvalidIndexDefinitionException(
          String.format(
              "Index can only use one embedding model. Found multiple models: %s. "
                  + "Please use a single embedding model across all auto-embedding fields.",
              String.join(", ", modelNames)));
    }
  }

  /**
   * Validates that the index does not mix regular vector fields with auto-embedding fields
   *
   * @param vectorIndex the vector index to validate
   * @throws InvalidIndexDefinitionException if the index mixes vector types
   */
  private static void validateNoMixedVectorTypes(VectorIndexDefinition vectorIndex)
      throws InvalidIndexDefinitionException {
    @Var boolean hasRegularVectorField = false;
    @Var boolean hasAutoEmbedField = false;

    for (VectorIndexFieldDefinition field : vectorIndex.getFields()) {
      VectorIndexFieldDefinition.Type type = field.getType();

      // Check for auto-embedding fields
      if (type == VectorIndexFieldDefinition.Type.TEXT
          || type == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
        hasAutoEmbedField = true;
      }

      // Check for regular vector fields
      if (type == VectorIndexFieldDefinition.Type.VECTOR) {
        hasRegularVectorField = true;
      }
    }

    if (hasRegularVectorField && hasAutoEmbedField) {
      throw new InvalidIndexDefinitionException(
          "Index cannot mix regular vector fields with auto-embedding fields. "
              + "Please use either pre-computed embeddings or auto-embedding, "
              + "but not both in the same index.");
    }
  }

  /**
   * Validates that an index update does not convert from auto-embedding to regular vector type.
   *
   * @param oldIndex the existing vector index definition
   * @param newIndex the updated vector index definition
   * @throws InvalidIndexDefinitionException if the update attempts to convert from autoEmbed to
   *     vector
   */
  public static void validateNoAutoEmbeddingTypeConversion(
      VectorIndexDefinition oldIndex, VectorIndexDefinition newIndex)
      throws InvalidIndexDefinitionException {

    boolean oldIsAutoEmbedding = oldIndex.isAutoEmbeddingIndex();
    boolean newIsAutoEmbedding = newIndex.isAutoEmbeddingIndex();

    if (oldIsAutoEmbedding && !newIsAutoEmbedding) {
      throw new InvalidIndexDefinitionException(
          "For a vector search index definition, you cannot convert a "
              + "type:autoEmbed to a type:vector");
    }
  }

  /**
   * Validates that auto-embedding fields have not been modified during an index update.
   *
   * @param oldIndex the existing vector index definition (must be an auto-embedding index)
   * @param newIndex the updated vector index definition (must be an auto-embedding index)
   * @throws InvalidIndexDefinitionException if auto-embedding fields have been modified
   * @throws IllegalArgumentException if either index is not an auto-embedding index
   */
  public static void validateNoAutoEmbeddingFieldChanges(
      VectorIndexDefinition oldIndex, VectorIndexDefinition newIndex)
      throws InvalidIndexDefinitionException {

    if (!oldIndex.isAutoEmbeddingIndex() || !newIndex.isAutoEmbeddingIndex()) {
      throw new IllegalArgumentException(
          "Unable to invoke auto-embedding field change validation on non auto-embedding indexes");
    }

    List<VectorIndexFieldDefinition> oldAutoEmbedFields = extractAutoEmbeddingFields(oldIndex);
    List<VectorIndexFieldDefinition> newAutoEmbedFields = extractAutoEmbeddingFields(newIndex);

    if (!oldAutoEmbedFields.equals(newAutoEmbedFields)) {
      throw new InvalidIndexDefinitionException(
          "Updates to auto-embedding fields are not allowed. "
              + "To modify auto-embedding fields, please drop and recreate the index.");
    }
  }

  /**
   * Extracts auto-embedding fields from a vector index definition.
   *
   * @param vectorIndex the vector index to extract fields from
   * @return a list of auto-embedding field definitions
   */
  private static List<VectorIndexFieldDefinition> extractAutoEmbeddingFields(
      VectorIndexDefinition vectorIndex) {
    return vectorIndex.getFields().stream()
        .filter(
            field ->
                // backward compatibility
                field.getType() == VectorIndexFieldDefinition.Type.TEXT
                    || field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .toList();
  }
}
