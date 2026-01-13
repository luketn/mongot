package com.xgen.testing.mongot.mock.index;

import com.xgen.mongot.index.VectorSearchResult;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonArray;
import org.bson.BsonInt32;

public class VectorSearchResultBatch {
  private final List<VectorSearchResult> results;

  public VectorSearchResultBatch(int batchSize) {
    this.results =
        IntStream.range(0, batchSize)
            .mapToObj(i -> new VectorSearchResult(new BsonInt32(i), i, Optional.empty()))
            .collect(Collectors.toList());
  }

  public VectorSearchResultBatch(List<VectorSearchResult> results) {
    this.results = results;
  }

  public BsonArray getBsonResults() {
    var values = this.results.stream().map(VectorSearchResult::toBson).collect(Collectors.toList());
    return new BsonArray(values);
  }
}
