package com.xgen.mongot.index.analyzer;

import org.apache.lucene.analysis.Analyzer;

@FunctionalInterface
public interface AnalyzerFactory<T> {
  Analyzer getAnalyzer(T definition);
}
