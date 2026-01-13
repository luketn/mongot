package com.xgen.testing.mongot.integration.index.serialization;

import java.util.List;
import org.bson.BsonArray;

public class IntermediateCursorResponse {

  private final List<TestSearchResult> testResults;
  private final BsonArray metaResults;

  public IntermediateCursorResponse(List<TestSearchResult> testResults, BsonArray metaResults) {
    this.testResults = testResults;
    this.metaResults = metaResults;
  }

  public List<TestSearchResult> getTestResults() {
    return this.testResults;
  }

  public BsonArray getMetaResults() {
    return this.metaResults;
  }
}
