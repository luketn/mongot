package com.xgen.mongot.index.definition;

import java.util.Objects;

/**
 * A vector field specification for text fields that require embedding model information.
 *
 * <p>Extends {@link VectorFieldSpecification} to include the name of the embedding model used to
 * generate vectors from text. This is used for auto-embedding features where text queries need to
 * be converted to vectors.
 */
public class VectorTextFieldSpecification extends VectorFieldSpecification {
  public static final String DEFAULT_MODALITY = "text";
  private final String modelName;
  private final String modality;

  public VectorTextFieldSpecification(
      int numDimensions,
      VectorSimilarity similarity,
      VectorQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm,
      String modelName) {
    super(numDimensions, similarity, quantization, indexingAlgorithm);
    this.modelName = modelName;
    this.modality = DEFAULT_MODALITY;
  }

  public VectorTextFieldSpecification(
      int numDimensions,
      VectorSimilarity similarity,
      VectorQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm,
      String modelName,
      String modality) {
    super(numDimensions, similarity, quantization, indexingAlgorithm);
    this.modelName = modelName;
    this.modality = modality;
  }

  /**
   * Returns the name of the embedding model used to generate vectors from text.
   *
   * @return the embedding model name
   */
  public String modelName() {
    return this.modelName;
  }

  /**
   * Returns the modality of data ingested into the vector field for auto-embedding index.
   *
   * @return the modality
   */
  public String modality() {
    return this.modality;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    VectorTextFieldSpecification that = (VectorTextFieldSpecification) o;
    return Objects.equals(this.modelName, that.modelName)
        && Objects.equals(this.modality, that.modality);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.modelName + this.modality);
  }
}
