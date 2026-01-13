package com.xgen.mongot.index.lucene.analyzer;

@FunctionalInterface
public interface AnalysisStep<T> {
  T create(T input);

  default AnalysisStep<T> andThen(AnalysisStep<T> after) {
    return (T t) -> after.create(create(t));
  }
}
