package com.xgen.testing.mongot.index.lucene.synonym;

import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import java.util.Collections;
import java.util.Optional;

public class SynonymRegistryBuilder {
  /** Create an empty synonym registry. */
  public static SynonymRegistry empty() {
    return LuceneSynonymRegistry.create(
        AnalyzerRegistryBuilder.empty(), Collections.emptyMap(), Optional.empty());
  }
}
