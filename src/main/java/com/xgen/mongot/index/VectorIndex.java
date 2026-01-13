package com.xgen.mongot.index;

import com.xgen.mongot.index.definition.VectorIndexDefinition;

public interface VectorIndex extends Index {

  @Override
  VectorIndexDefinition getDefinition();
}
