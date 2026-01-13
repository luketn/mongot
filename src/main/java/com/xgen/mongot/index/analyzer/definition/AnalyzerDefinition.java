package com.xgen.mongot.index.analyzer.definition;

public sealed interface AnalyzerDefinition
    permits CustomAnalyzerDefinition, NormalizerDefinition, OverriddenBaseAnalyzerDefinition {

  String name();
}
