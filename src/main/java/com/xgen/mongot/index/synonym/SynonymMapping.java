package com.xgen.mongot.index.synonym;

import org.apache.lucene.analysis.Analyzer;

/**
 * A {@link SynonymMapping} is an entry in the {@link SynonymRegistry}. It holds two pieces of
 * information - an analyzer configured to apply synonyms, and the name of the baseAnalyzer that the
 * synonym-configured analyzer was based on.
 */
public class SynonymMapping {
  public final Analyzer analyzer;
  public final String baseAnalyzerName;

  /**
   * A {@link Builder} is responsible for building {@link SynonymMapping}s from a series of {@link
   * SynonymDocument}s.
   */
  public interface Builder {
    Builder addDocument(SynonymDocument document) throws SynonymMappingException;

    SynonymMapping build() throws SynonymMappingException;
  }

  public SynonymMapping(Analyzer analyzer, String baseAnalyzerName) {
    this.analyzer = analyzer;
    this.baseAnalyzerName = baseAnalyzerName;
  }
}
