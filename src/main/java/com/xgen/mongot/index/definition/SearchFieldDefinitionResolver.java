package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

/**
 * FieldDefinitionResolver retrieves the correct FieldDefinition for a combination of path +
 * embeddedRoot by checking to see if there exists a statically defined FieldDefinition else
 * defaulting to the IndexFormatVersion-specific dynamic FieldDefinition.
 */
public class SearchFieldDefinitionResolver implements FieldDefinitionResolver {

  public final SearchIndexDefinition indexDefinition;
  public final IndexCapabilities indexCapabilities;

  SearchFieldDefinitionResolver(
      SearchIndexDefinition indexDefinition, IndexFormatVersion indexFormatVersion) {
    this.indexDefinition = indexDefinition;
    this.indexCapabilities = indexDefinition.getIndexCapabilities(indexFormatVersion);
  }

  public Optional<FieldDefinition> getFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return embeddedRoot
        .flatMap(
            rootPath ->
                this.indexDefinition
                    .getMappings()
                    .fieldHierarchyContext()
                    .getFieldDefinition(path, rootPath))
        .or(() -> resolveFieldDefinition(path, embeddedRoot));
  }

  public Optional<StringFieldDefinition> getStringFieldDefinition(
      StringPath stringPath, Optional<FieldPath> embeddedRoot) {
    Optional<StringFieldDefinition> baseStringFieldDefinition =
        getFieldDefinition(stringPath.getBaseFieldPath(), embeddedRoot)
            .flatMap(FieldDefinition::stringFieldDefinition);

    if (stringPath.isField()) {
      return baseStringFieldDefinition;
    }

    return baseStringFieldDefinition.flatMap(
        field -> field.getMultiDefinition(stringPath.asMultiField().getMulti()));
  }

  /**
   * Fetches the EmbeddedDocumentsFieldDefinition for an embeddedRoot if present, empty if not
   *
   * @param embeddedRoot EmbeddedRoot path
   * @return EmbeddedDocumentsFieldDefinition
   */
  public Optional<EmbeddedDocumentsFieldDefinition> getEmbeddedDocumentsFieldDefinition(
      FieldPath embeddedRoot) {
    return Optional.ofNullable(
        this.indexDefinition
            .getFieldHierarchyContext()
            .getEmbeddedFieldDefinitionByEmbeddedRoot()
            .get(embeddedRoot));
  }

  @Override
  public boolean isUsed(FieldPath path) {
    return getFieldDefinition(path, Optional.empty()).isPresent()
        || this.indexDefinition
            .getMappings()
            .fieldHierarchyContext()
            .getEmbeddedDocumentsRelativeRoots()
            .stream()
            .anyMatch(
                embeddedRoot -> getFieldDefinition(path, Optional.of(embeddedRoot)).isPresent())
        || this.indexDefinition.getStoredSource().isPathToStored(path);
  }

  @Override
  public Optional<VectorFieldSpecification> getVectorFieldSpecification(FieldPath path) {
    return getFieldDefinition(path, Optional.empty())
        .flatMap(FieldDefinition::vectorFieldSpecification);
  }

  public IndexCapabilities getIndexCapabilities() {
    return this.indexCapabilities;
  }

  /**
   * Returns the correct <code>dynamic</code> {@link FieldDefinition} based on the {@link
   * DynamicDefinition} supplied.
   */
  public Optional<FieldDefinition> getDynamicFieldDefinition(
      HierarchicalFieldDefinition hierarchicalFieldDefinition) {
    return switch (hierarchicalFieldDefinition.dynamic()) {
      case DynamicDefinition.Boolean bool ->
          bool.value() ? Optional.of(FieldDefinition.DYNAMIC_FIELD_DEFINITION) : Optional.empty();

      case DynamicDefinition.Document document -> {
        // If dynamicDefinition is of type document, validation has already occurred during
        // deserialization that it references a unique valid typeSet defined in the IndexDefinition
        Optional<TypeSetDefinition> matchingTypeSet =
            Optional.ofNullable(this.indexDefinition.getTypeSetsMap().get(document.typeSet()));
        yield Optional.of(Check.isPresent(matchingTypeSet, "matchingTypeSet").getFieldDefinition());
      }
    };
  }

  private Optional<FieldDefinition> resolveFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    Optional<FieldDefinition> staticDefinition = getStaticDefinition(path, embeddedRoot);
    if (staticDefinition.isPresent()) {
      return staticDefinition;
    }

    // The path is not statically configured, so we need to see if it is dynamically configured.
    // Find the closest ancestor (starting with the parent) that has a configured FieldDefinition.
    Optional<FieldDefinition> optionalAncestorDefinition =
        path.ancestorPaths()
            .map(ancestorPath -> getStaticDefinition(ancestorPath, embeddedRoot))
            .flatMap(Optional::stream)
            .findFirst();

    // If we didn't find an ancestor of the path, fall back to the root mappings.
    if (optionalAncestorDefinition.isEmpty()) {
      if (embeddedRoot.isEmpty()) {
        return getDynamicFieldDefinition(this.indexDefinition.getMappings());
      }

      return Optional.ofNullable(
              this.indexDefinition
                  .getMappings()
                  .fieldHierarchyContext()
                  .getRootFields()
                  .get(embeddedRoot.get()))
          .flatMap(FieldDefinition::embeddedDocumentsFieldDefinition)
          .flatMap(this::getDynamicFieldDefinition);
    }

    FieldDefinition ancestorDefinition = optionalAncestorDefinition.get();
    Optional<DocumentFieldDefinition> optionalDocumentDefinition =
        ancestorDefinition.documentFieldDefinition();

    // If the statically configured ancestor was not configured to be a document, then this field
    // should not be resolved.
    if (optionalDocumentDefinition.isEmpty()) {
      return Optional.empty();
    }

    // Finally, if we found a statically configured ancestor which is configured to be a document,
    // check whether it is configured to have its children be dynamically discovered.
    DocumentFieldDefinition documentDefinition = optionalDocumentDefinition.get();
    return getDynamicFieldDefinition(documentDefinition);
  }

  private Optional<FieldDefinition> getStaticDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    if (embeddedRoot.isPresent()) {
      return this.indexDefinition
          .getMappings()
          .fieldHierarchyContext()
          .getFieldDefinition(path, embeddedRoot.get());
    }

    return Optional.ofNullable(this.indexDefinition.getStaticFields().get(path.toString()));
  }

  public ImmutableMap<String, FieldDefinition> getFields() {
    return this.indexDefinition.getMappings().fields();
  }
}
