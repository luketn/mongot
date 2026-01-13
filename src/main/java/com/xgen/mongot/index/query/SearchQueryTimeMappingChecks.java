package com.xgen.mongot.index.query;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.GeoFieldDefinition;
import com.xgen.mongot.index.definition.KnnVectorFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;

/** Helps check that paths are mapped correctly to service some operator. */
public class SearchQueryTimeMappingChecks implements QueryTimeMappingChecks {
  private final SearchFieldDefinitionResolver fieldDefinitionResolver;

  public SearchQueryTimeMappingChecks(SearchFieldDefinitionResolver fieldDefinitionResolver) {
    this.fieldDefinitionResolver = fieldDefinitionResolver;
  }

  @Override
  public boolean indexedAsGeoPoint(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::geoFieldDefinition)
        .isPresent();
  }

  /** Path stores geo shapes. */
  @Override
  public boolean indexedAsGeoShape(FieldPath path, Optional<FieldPath> embeddedRoot) {
    Optional<GeoFieldDefinition> geoFieldDefinition =
        this.fieldDefinitionResolver
            .getFieldDefinition(path, embeddedRoot)
            .flatMap(FieldDefinition::geoFieldDefinition);
    return geoFieldDefinition.isPresent() && geoFieldDefinition.get().indexShapes();
  }

  /** Validate that the path has proper storage as a string field in the index. */
  @Override
  public void validatePathStringStorage(StringPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    // TODO(CLOUDP-280897): should highlighting be over disjunction of paths with embedded root?
    if (isHighlightableStringField(path, embeddedRoot)) {
      return;
    }

    if (this.fieldDefinitionResolver.getStringFieldDefinition(path, embeddedRoot).isPresent()) {
      throw new InvalidQueryException(
          "Highlights can not be generated for path:"
              + path
              + ". Index definition specifies store:false");
    }
    throw new InvalidQueryException(
        String.format(
            "Highlights cannot be generated. Path: \"%s\" "
                + "is not stored statically or dynamically indexed.",
            path));
  }

  public void validateKnnSimilarityIsCalculable(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot, float[] vector)
      throws InvalidQueryException {
    Optional<KnnVectorFieldDefinition> definition =
        this.fieldDefinitionResolver
            .getFieldDefinition(fieldPath, embeddedRoot)
            .flatMap(FieldDefinition::knnVectorFieldDefinition);

    // This code path comes from the KnnBetaOperator which does not support BSON vectors, hence
    // hard-coding the OriginalType to NATIVE below.
    if (definition.get().specification().similarity() == VectorSimilarity.COSINE
        && Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE).isZeroVector()) {
      throw new InvalidQueryException(
          "Cosine similarity cannot be calculated against a zero knnVector.");
    }
  }

  /**
   * Validates knnVector field. Used by KnnBetaOperator.
   *
   * @deprecated to be removed when knnVector field type support is dropped, use
   *     'validateVectorField' instead.
   */
  @Deprecated
  public void validateKnnVectorField(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot, int dimensions)
      throws InvalidQueryException {
    Optional<VectorFieldSpecification> definition =
        this.fieldDefinitionResolver
            .getFieldDefinition(fieldPath, embeddedRoot)
            .flatMap(FieldDefinition::vectorFieldSpecification);

    if (definition.isEmpty()) {
      throw new InvalidQueryException(String.format("%s is not indexed as knnVector", fieldPath));
    }
    if (definition.get().numDimensions() != dimensions) {
      throw new InvalidQueryException(
          String.format(
              "knnVector field is indexed with %s dimensions but queried with %s",
              definition.get().numDimensions(), dimensions));
    }
  }

  @Override
  public void validateVectorField(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot, int dimensions)
      throws InvalidQueryException {
    Optional<VectorFieldSpecification> definition =
        this.fieldDefinitionResolver
            .getFieldDefinition(fieldPath, embeddedRoot)
            .flatMap(FieldDefinition::vectorFieldSpecification);

    if (definition.isEmpty()) {
      throw new InvalidQueryException(
          String.format("%s must be indexed as \"vector\".", fieldPath));
    }
    if (definition.get().numDimensions() != dimensions) {
      throw new InvalidQueryException(
          String.format(
              "vector field is indexed with %s dimensions but queried with %s",
              definition.get().numDimensions(), dimensions));
    }
  }

  @Override
  public boolean isStringFieldIndexed(StringFieldPath fieldPath, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getStringFieldDefinition(fieldPath, embeddedRoot)
        .isPresent();
  }

  public boolean isStringFieldIndexedWithPositionInfo(
      StringPath path, Optional<FieldPath> embeddedRoot) {
    Optional<StringFieldDefinition> stringFieldDefinition =
        this.fieldDefinitionResolver.getStringFieldDefinition(path, embeddedRoot);
    return stringFieldDefinition.isPresent()
        && stringFieldDefinition.get().indexOptions().containsPositionInfo();
  }

  public void validateStringFieldIsIndexedWithPositionInfo(
      StringPath path, Optional<FieldPath> embeddedRoot, String errorMessage)
      throws InvalidQueryException {
    Optional<StringFieldDefinition> stringFieldDefinition =
        this.fieldDefinitionResolver.getStringFieldDefinition(path, embeddedRoot);
    if (stringFieldDefinition.isPresent()
        && !stringFieldDefinition.get().indexOptions().containsPositionInfo()) {
      throw new InvalidQueryException(errorMessage);
    }
  }

  /** Checks whether the given path is indexed as string. */
  @Override
  public boolean isHighlightableStringField(StringPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getStringFieldDefinition(path, embeddedRoot)
        .map(StringFieldDefinition::storeFlag)
        .orElse(false);
  }

  @Override
  public boolean isIndexedAsEmbeddedDocumentsField(FieldPath fieldPath) {
    return this.fieldDefinitionResolver.indexDefinition.isIndexedAsEmbeddedDocumentsField(
        fieldPath);
  }

  @Override
  public boolean indexedAsDate(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::dateFieldDefinition)
        .isPresent();
  }

  @Override
  public boolean indexedAsNumber(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::numberFieldDefinition)
        .isPresent();
  }

  @Override
  public boolean indexedAsToken(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::tokenFieldDefinition)
        .isPresent();
  }

  @Override
  public boolean indexedAsBoolean(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::booleanFieldDefinition)
        .isPresent();
  }

  @Override
  public boolean indexedAsObjectId(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::objectIdFieldDefinition)
        .isPresent();
  }

  @Override
  public boolean indexedAsUuid(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver
        .getFieldDefinition(path, embeddedRoot)
        .flatMap(FieldDefinition::uuidFieldDefinition)
        .isPresent();
  }

  /**
   * Checks whether the given path is indexed statically or dynamically, since such fields support
   * indexing null values.
   */
  public boolean indexesNulls(FieldPath path, Optional<FieldPath> embeddedRoot) {
    return this.fieldDefinitionResolver.getFieldDefinition(path, embeddedRoot).isPresent();
  }

  @Override
  public boolean supportsFilter() {
    return false;
  }
}
