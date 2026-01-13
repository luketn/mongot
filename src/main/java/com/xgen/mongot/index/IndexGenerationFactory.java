package com.xgen.mongot.index;

import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import java.io.IOException;

public class IndexGenerationFactory {
  public static IndexGeneration getIndexGeneration(
      IndexFactory indexFactory, IndexDefinitionGeneration definitionGeneration)
      throws IOException, InvalidAnalyzerDefinitionException {
    Index index = indexFactory.getIndex(definitionGeneration);
    return new IndexGeneration(index, definitionGeneration);
  }
}
