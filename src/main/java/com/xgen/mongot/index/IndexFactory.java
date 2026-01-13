package com.xgen.mongot.index;

import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import java.io.Closeable;
import java.io.IOException;

/** An IndexFactory is able to create a new Index from its definition. */
public interface IndexFactory extends Closeable {

  /** Creates a new index. Can result in errors if index is already created. */
  Index getIndex(IndexDefinitionGeneration definitionGeneration)
      throws IOException, InvalidAnalyzerDefinitionException;

  /**
   * Creates a new InitializedIndex for the specified index. Can result in errors if index is
   * already initialized.
   */
  InitializedIndex getInitializedIndex(Index index, IndexDefinitionGeneration definitionGeneration)
      throws IOException;

  @Override
  void close();
}
