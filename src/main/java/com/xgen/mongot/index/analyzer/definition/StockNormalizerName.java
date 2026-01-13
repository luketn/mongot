package com.xgen.mongot.index.analyzer.definition;

public enum StockNormalizerName {
  NONE("none"),
  LOWERCASE("lowercase");
  private final String normalizerName;

  StockNormalizerName(String normalizerName) {
    this.normalizerName = normalizerName;
  }

  public String getNormalizerName() {
    return this.normalizerName;
  }
}
