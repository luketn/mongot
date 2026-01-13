package com.xgen.mongot.index.definition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link FieldHierarchyContext} aggregates information about children of a {@link
 * FieldTypeDefinition}. The {@link FieldHierarchyContext} of the {@link DocumentFieldDefinition} at
 * the root of an {@link SearchIndexDefinition} summarizes "hierarchical information" of the entire
 * index definition.
 *
 * <p>Examples of "hierarchical information" include the depth of the most deeply-layered {@link
 * EmbeddedDocumentsFieldDefinition}, and the relative {@link FieldPath}s of {@link
 * EmbeddedDocumentsFieldDefinition}s under a particular field.
 */
public class FieldHierarchyContext {
  /**
   * Maps embedded root path to {@link EmbeddedDocumentsFieldDefinition} field definitions for all
   * field definitions containing an embedded documents field.
   */
  private final ImmutableMap<FieldPath, FieldDefinition> rootFields;

  /**
   * Maps embedded root path to a map from {@link FieldPath} to {@link FieldDefinition} for all
   * statically configured fields of an {@link EmbeddedDocumentsFieldDefinition}.
   */
  private final ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>>
      fieldsByEmbeddedRoot;

  private final ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition>
      embeddedFieldDefinitionByEmbeddedRoot;

  /**
   * The number of embeddedDocuments "layers" of the most-deeply layered embeddedDocuments
   * descendent of this field. See {@link FieldHierarchyContext#getNumEmbeddedDocumentsLayers()} for
   * more details.
   */
  private final int numEmbeddedDocumentsLayers;

  FieldHierarchyContext(
      ImmutableMap<FieldPath, FieldDefinition> rootFields,
      ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot,
      ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition>
          embeddedFieldDefinitionByEmbeddedRoot,
      int numEmbeddedDocumentsLayers) {
    this.rootFields = rootFields;
    this.fieldsByEmbeddedRoot = fieldsByEmbeddedRoot;
    this.embeddedFieldDefinitionByEmbeddedRoot = embeddedFieldDefinitionByEmbeddedRoot;
    this.numEmbeddedDocumentsLayers = numEmbeddedDocumentsLayers;
  }

  /**
   * An {@link EmbeddedDocumentsFieldDefinition} field may be the child of another {@link
   * EmbeddedDocumentsFieldDefinition}. An index definition with an {@link
   * EmbeddedDocumentsFieldDefinition} that itself has an {@link EmbeddedDocumentsFieldDefinition}
   * field as a child is said to have two "embeddedDocuments layers".
   *
   * <p>Atlas Search imposes limits on how many layers of {@link EmbeddedDocumentsFieldDefinition}s
   * may be in a single index definition.
   *
   * <p>{@link FieldHierarchyContext#getNumEmbeddedDocumentsLayers()} returns the number of
   * embeddedDocuments "layers" between the most-deeply layered embeddedDocuments descendent of this
   * field and this field itself. Said differently, this number represents the number of
   * embeddedDocuments layers between this field and the most-layered embeddedDocuments field that
   * is a descendent of it.
   */
  int getNumEmbeddedDocumentsLayers() {
    return this.numEmbeddedDocumentsLayers;
  }

  ImmutableSet<FieldPath> getEmbeddedDocumentsRelativeRoots() {
    return this.fieldsByEmbeddedRoot.keySet();
  }

  public ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>>
      getFieldsByEmbeddedRoot() {
    return this.fieldsByEmbeddedRoot;
  }

  public ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition>
      getEmbeddedFieldDefinitionByEmbeddedRoot() {
    return this.embeddedFieldDefinitionByEmbeddedRoot;
  }

  public ImmutableMap<FieldPath, FieldDefinition> getRootFields() {
    return this.rootFields;
  }

  Optional<FieldDefinition> getFieldDefinition(FieldPath fieldPath, FieldPath embeddedRoot) {
    return Optional.ofNullable(this.fieldsByEmbeddedRoot.get(embeddedRoot))
        .map(fieldMap -> fieldMap.get(fieldPath));
  }

  /**
   * Create for a {@link DocumentFieldDefinition}. Throws a {@link IllegalEmbeddedFieldException} if
   * that field has two {@link EmbeddedDocumentsFieldDefinition} children at the same path.
   */
  static FieldHierarchyContext createForDocumentsField(Map<String, FieldDefinition> fields)
      throws IllegalEmbeddedFieldException {
    return create(fields, false);
  }

  /**
   * Create for an {@link EmbeddedDocumentsFieldDefinition}. Throws a {@link
   * IllegalEmbeddedFieldException} if that field has two {@link EmbeddedDocumentsFieldDefinition}
   * children at the same path.
   */
  static FieldHierarchyContext createForEmbeddedDocumentsField(Map<String, FieldDefinition> fields)
      throws IllegalEmbeddedFieldException {
    return create(fields, true);
  }

  private static FieldHierarchyContext create(
      Map<String, FieldDefinition> fields, boolean isEmbeddedDocs)
      throws IllegalEmbeddedFieldException {
    Map<FieldPath, Map<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot = new HashMap<>();
    Map<FieldPath, FieldDefinition> rootFields = new HashMap<>();
    Map<FieldPath, EmbeddedDocumentsFieldDefinition> embeddedFieldDefinitionByEmbeddedRoot =
        new HashMap<>();
    CheckedStream.from(fields.entrySet())
        .forEachChecked(
            field ->
                populateRelativeRoots(
                    field.getKey(),
                    field.getValue(),
                    fieldsByEmbeddedRoot,
                    rootFields,
                    embeddedFieldDefinitionByEmbeddedRoot));

    int numEmbeddedDocumentLayers =
        fields.values().stream()
                .mapToInt(FieldHierarchyContext::getNumEmbeddedLayers)
                .max()
                .orElse(0)
            + (isEmbeddedDocs ? 1 : 0);

    return new FieldHierarchyContext(
        ImmutableMap.copyOf(rootFields),
        fieldsByEmbeddedRoot.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue()))),
        ImmutableMap.copyOf(embeddedFieldDefinitionByEmbeddedRoot),
        numEmbeddedDocumentLayers);
  }

  @VisibleForTesting
  static void populateRelativeRoots(
      String leafPath,
      FieldDefinition field,
      Map<FieldPath, Map<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot,
      Map<FieldPath, FieldDefinition> rootFields,
      Map<FieldPath, EmbeddedDocumentsFieldDefinition> embeddedFieldDefinitionByEmbeddedRoot)
      throws IllegalEmbeddedFieldException {
    Set<FieldPath> embeddedDocumentsRelativeRoots =
        field.embeddedDocumentsFieldDefinition().stream()
            .map(EmbeddedDocumentsFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getEmbeddedDocumentsRelativeRoots)
            .flatMap(Collection::stream)
            .map(relativeRoot -> relativeRoot.withNewRoot(leafPath))
            .collect(Collectors.toSet());
    Set<FieldPath> documentRelativeRoots =
        field.documentFieldDefinition().stream()
            .map(DocumentFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getEmbeddedDocumentsRelativeRoots)
            .flatMap(Collection::stream)
            .map(relativeRoot -> relativeRoot.withNewRoot(leafPath))
            .collect(Collectors.toSet());

    Set<FieldPath> conflictingRelativeRoots =
        Sets.intersection(embeddedDocumentsRelativeRoots, documentRelativeRoots);

    if (!conflictingRelativeRoots.isEmpty()) {
      throw IllegalEmbeddedFieldException.withConflictingRelativeRoots(conflictingRelativeRoots);
    }

    // For embedded field, get all embedded fields maps, prepend new root path, and add to field by
    // embedded root map for this node.
    populateFieldsByEmbeddedRoot(
        field.embeddedDocumentsFieldDefinition(),
        leafPath,
        fieldsByEmbeddedRoot,
        embeddedFieldDefinitionByEmbeddedRoot);

    // For document field, get all embedded fields maps, prepend new root path, and add to field by
    // embedded root map for this node.
    populateFieldsByEmbeddedRoot(
        field.documentFieldDefinition(),
        leafPath,
        fieldsByEmbeddedRoot,
        embeddedFieldDefinitionByEmbeddedRoot);

    // for embedded field - add "rootFields" map as value in fieldsByEmbeddedRoot map after
    // prepending leaf path
    field
        .embeddedDocumentsFieldDefinition()
        .map(EmbeddedDocumentsFieldDefinition::fieldHierarchyContext)
        .map(FieldHierarchyContext::getRootFields)
        .ifPresent(
            embeddedHierarchyFields ->
                fieldsByEmbeddedRoot.put(
                    FieldPath.newRoot(leafPath), prependPaths(embeddedHierarchyFields, leafPath)));
    field
        .embeddedDocumentsFieldDefinition()
        .ifPresent(
            fieldDefinition ->
                embeddedFieldDefinitionByEmbeddedRoot.put(
                    FieldPath.newRoot(leafPath), fieldDefinition));
    // for document field - add "rootFields" map as value to rootFields map after prepending leaf
    // path
    field
        .documentFieldDefinition()
        .map(DocumentFieldDefinition::fieldHierarchyContext)
        .map(FieldHierarchyContext::getRootFields)
        .ifPresent(
            documentHierarchyFields ->
                rootFields.putAll(prependPaths(documentHierarchyFields, leafPath)));

    rootFields.put(FieldPath.newRoot(leafPath), field);
  }

  private static void populateFieldsByEmbeddedRoot(
      Optional<? extends HierarchicalFieldDefinition> hierarchicalFieldDefinition,
      String leafPath,
      Map<FieldPath, Map<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot,
      Map<FieldPath, EmbeddedDocumentsFieldDefinition> embeddedRootDocuments) {
    if (hierarchicalFieldDefinition.isEmpty()) {
      return;
    }

    hierarchicalFieldDefinition
        .get()
        .fieldHierarchyContext()
        .getFieldsByEmbeddedRoot()
        .forEach(
            (embeddedRootPath, embeddedFieldMap) ->
                fieldsByEmbeddedRoot.put(
                    embeddedRootPath.withNewRoot(leafPath),
                    prependPaths(embeddedFieldMap, leafPath)));

    hierarchicalFieldDefinition
        .get()
        .fieldHierarchyContext()
        .getEmbeddedFieldDefinitionByEmbeddedRoot()
        .forEach(
            (embeddedRootPath, embeddedDocumentsFieldDefinition) ->
                embeddedRootDocuments.put(
                    embeddedRootPath.withNewRoot(leafPath), embeddedDocumentsFieldDefinition));
  }

  private static <T> ImmutableMap<FieldPath, T> prependPaths(
      Map<FieldPath, T> fieldMap, String newRootPath) {
    return fieldMap.entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                entry -> entry.getKey().withNewRoot(newRootPath), Map.Entry::getValue));
  }

  private static int getNumEmbeddedLayers(FieldDefinition field) {
    return Math.max(
        field
            .embeddedDocumentsFieldDefinition()
            .map(EmbeddedDocumentsFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getNumEmbeddedDocumentsLayers)
            .orElse(0),
        field
            .documentFieldDefinition()
            .map(DocumentFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getNumEmbeddedDocumentsLayers)
            .orElse(0));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldHierarchyContext that = (FieldHierarchyContext) o;
    return this.numEmbeddedDocumentsLayers == that.numEmbeddedDocumentsLayers
        && this.rootFields.equals(that.rootFields)
        && this.fieldsByEmbeddedRoot.equals(that.fieldsByEmbeddedRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.rootFields, this.fieldsByEmbeddedRoot, this.numEmbeddedDocumentsLayers);
  }
}
