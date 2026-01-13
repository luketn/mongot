package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.utils.ReplaceStringsFieldValueHandler.HASH_FIELD_SUFFIX;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexVectorFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class AutoEmbeddingIndexDefinitionUtils {

  // takes the raw vector index definition and replaces the collection UUID and auto-embedding
  // fields
  public static VectorIndexDefinition getDerivedVectorIndexDefinition(
      VectorIndexDefinition rawDefinition,
      String databaseName,
      UUID materializedViewCollectionUuid) {
    return new VectorIndexDefinition(
        rawDefinition.getIndexId(),
        rawDefinition.getName(),
        databaseName,
        rawDefinition.getIndexId().toHexString(),
        materializedViewCollectionUuid,
        Optional.empty(),
        rawDefinition.getNumPartitions(),
        getDerivedVectorIndexFields(rawDefinition.getFields()),
        rawDefinition.getParsedIndexFeatureVersion(),
        rawDefinition.getDefinitionVersion(),
        rawDefinition.getDefinitionVersionCreatedAt(),
        Optional.empty()); // TODO(https://jira.mongodb.org/browse/CLOUDP-363302)
  }

  // replaces the auto-embedding text field with a vector field.
  private static List<VectorIndexFieldDefinition> getDerivedVectorIndexFields(
      ImmutableList<VectorIndexFieldDefinition> rawFields) {
    return rawFields.stream()
        .map(
            field -> {
              if (field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
                VectorIndexVectorFieldDefinition autoEmbedField = field.asVectorField();
                return new VectorDataFieldDefinition(
                    field.getPath(), autoEmbedField.specification());
              }
              return field;
            })
        .toList();
  }

  // adds the auto-embedding hash field paths to the raw field mappings.
  public static VectorIndexFieldMapping getMatViewIndexFields(
      VectorIndexFieldMapping rawFieldMapping) {
    var updatedFieldMap = new HashMap<>(rawFieldMapping.fieldMap());
    rawFieldMapping.fieldMap().entrySet().stream()
        .filter(field -> field.getValue().getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .forEach(
            field -> {
              updatedFieldMap.put(getHashFieldPath(field.getKey()), field.getValue());
            });
    return new VectorIndexFieldMapping(
        ImmutableMap.copyOf(updatedFieldMap), rawFieldMapping.documentPaths());
  }

  /**
   * Returns the hash field path for the given field path. Covers two cases:
   *
   * <p>1. If the field path is at the root, then the hash field path is simply the field path with
   * the hash field suffix appended. So a field path "a" would become "a_hash".
   *
   * <p>2. If the field path is nested, then the hash field path is a path at the same level with
   * the hash field suffix appended. So a field path "a.b.c" would become "a.b.c_hash".
   *
   * @param fieldPath the current field path
   * @return the hash field path corresponding to the given field path
   */
  public static FieldPath getHashFieldPath(FieldPath fieldPath) {
    return fieldPath
        .getParent()
        .map(path -> path.newChild(fieldPath.getLeaf() + HASH_FIELD_SUFFIX))
        .orElse(FieldPath.newRoot(fieldPath.getLeaf() + HASH_FIELD_SUFFIX));
  }
}
