package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public class CursorResponse {

  private final List<TestSearchResult> testSearchResults;
  private final Optional<MetaResults> metaResults;

  public CursorResponse(List<TestSearchResult> testSearchResults, Optional<BsonValue> metaResults)
      throws BsonParseException {
    this.testSearchResults = testSearchResults;
    this.metaResults =
        metaResults.isPresent()
            ? Optional.of(MetaResults.fromBson(metaResults.get().asDocument()))
            : Optional.empty();
  }

  public List<TestSearchResult> getTestResults() {
    return this.testSearchResults;
  }

  public Optional<MetaResults> getMetaResults() {
    return this.metaResults;
  }
}
