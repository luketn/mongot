package com.xgen.testing.mongot.integration.index.serialization;

public enum SearchStage {
  SEARCH("$search", "searchScore"),
  VECTOR_SEARCH("$vectorSearch", "vectorSearchScore");

  private final String name;
  private final String scoreFieldName;

  SearchStage(String name, String scoreFieldName) {
    this.name = name;
    this.scoreFieldName = scoreFieldName;
  }

  public String getName() {
    return this.name;
  }

  public String getScoreFieldName() {
    return this.scoreFieldName;
  }
}
