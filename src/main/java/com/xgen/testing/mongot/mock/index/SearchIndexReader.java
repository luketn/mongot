package com.xgen.testing.mongot.mock.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.query.InvalidQueryException;
import java.io.IOException;
import java.util.Optional;

public class SearchIndexReader {

  /** Returns an IndexReader that can be used in tests that require one. */
  public static com.xgen.mongot.index.SearchIndexReader mockIndexReader() {
    return mockIndexReader(Optional.empty());
  }

  public static com.xgen.mongot.index.SearchIndexReader mockIndexReader(int numFullBatches) {
    return mockIndexReader(Optional.of(numFullBatches));
  }

  static com.xgen.mongot.index.SearchIndexReader mockIndexReader(Optional<Integer> numFullBatches) {
    var indexReader = mock(com.xgen.mongot.index.SearchIndexReader.class);

    try {
      when(indexReader.query(any(), any(), any(), any()))
          .thenAnswer(ignored -> BatchProducer.mockSearchResultBatchProducer(numFullBatches));
      when(indexReader.intermediateQuery(any(), any(), any(), any()))
          .thenAnswer(
              ignored -> BatchProducer.mockIntermediateSearchResultBatchProducer(numFullBatches));
    } catch (IOException | InvalidQueryException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return indexReader;
  }

  static com.xgen.mongot.index.SearchIndexReader mockIndexReaderWithMetaProducer(
      Optional<Integer> numFullBatches, Optional<Integer> numMetaBatches) {
    var indexReader = mock(com.xgen.mongot.index.SearchIndexReader.class);

    try {
      when(indexReader.intermediateQuery(any(), any(), any(), any()))
          .thenAnswer(
              ignored ->
                  BatchProducer.mockIntermediateSearchResultBatchProducer(
                      numFullBatches, numMetaBatches));
    } catch (IOException | InvalidQueryException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return indexReader;
  }
}
