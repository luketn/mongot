package com.xgen.testing.mongot.mock.index;

import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.index.SearchResult;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonArray;
import org.bson.BsonInt32;

public class SearchResultBatch {

  private final List<SearchResult> results;

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public SearchResultBatch(int batchSize) {
    var highlights =
        List.of(
            new SearchHighlight(
                1,
                StringPathBuilder.fieldPath("foo"),
                Collections.singletonList(
                    new SearchHighlightText("bar", SearchHighlightText.Type.TEXT))));
    this.results =
        IntStream.range(0, batchSize)
            .mapToObj(
                i ->
                    new SearchResult(
                        Optional.of(new BsonInt32(i)),
                        i,
                        Optional.of(highlights),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
            .collect(Collectors.toList());
  }

  public SearchResultBatch(List<SearchResult> results) {
    this.results = results;
  }

  public BsonArray getBsonResults() {
    var values = this.results.stream().map(SearchResult::toBson).collect(Collectors.toList());
    return new BsonArray(values);
  }
}
