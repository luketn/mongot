package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public interface FieldDefinitionResolver {

  /**
   * Returns true if the provided path is indexed or is a part of path to an indexed node. For
   * example if path "a.b.c" is indexed as a certain type, paths "a", "a.b" and "a.b.c" are used.
   */
  boolean isUsed(FieldPath path);

  Optional<VectorFieldSpecification> getVectorFieldSpecification(FieldPath path);
}
