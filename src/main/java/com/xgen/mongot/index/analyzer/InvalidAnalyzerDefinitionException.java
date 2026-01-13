package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.util.LoggableException;

public class InvalidAnalyzerDefinitionException extends LoggableException {

  InvalidAnalyzerDefinitionException(String s) {
    super(s);
  }

  public static InvalidAnalyzerDefinitionException emptyAnalyzerName() {
    return new InvalidAnalyzerDefinitionException("analyzer name is empty");
  }

  static InvalidAnalyzerDefinitionException analyzerNotFound(String analyzerName) {
    return new InvalidAnalyzerDefinitionException(
        String.format("analyzer %s not found", analyzerName));
  }

  static InvalidAnalyzerDefinitionException nameClashesWithStockAnalyzer(String analyzerName) {
    return new InvalidAnalyzerDefinitionException(
        String.format(
            "Analyzer \"%s\" is a stock analyzer and must be "
                + "given a different name when overridden.",
            analyzerName));
  }
}
