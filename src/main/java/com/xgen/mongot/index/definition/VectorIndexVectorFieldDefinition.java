package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;

/**
 * Abstract base class for vector field definitions in a vector search index.
 *
 * <p>This class provides a common structure for all vector field types, requiring each
 * implementation to provide a {@link VectorFieldSpecification} that defines the vector space
 * parameters (dimensions, similarity function, quantization, etc.).
 */
public abstract class VectorIndexVectorFieldDefinition extends VectorIndexFieldDefinition {

  public VectorIndexVectorFieldDefinition(FieldPath path) {
    super(path);
  }

  public abstract VectorFieldSpecification specification();
}
