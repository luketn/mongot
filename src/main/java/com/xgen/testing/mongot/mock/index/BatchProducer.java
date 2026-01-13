package com.xgen.testing.mongot.mock.index;

import static org.mockito.Mockito.spy;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.index.CountMetaBatchProducer;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.util.Bytes;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.bson.BsonArray;

public class BatchProducer {

  // Must be the same as the real batch size, as it is used to determine when a cursor is exhausted
  public static final int MOCK_BATCH_SIZE = BatchSizeStrategy.DEFAULT_BATCH_SIZE;
  public static final int REMAINING_DOCS_LAST_BATCH = 2;
  public static final int EXPECTED_DEFAULT_DOC_COUNT = 305;

  private static class MockProducer implements com.xgen.mongot.index.BatchProducer {

    private final int batchesToProduce;
    private int batchesProduced;

    private MockProducer(int numResults) {
      this.batchesToProduce = numResults;
      this.batchesProduced = 0;
    }

    @Override
    public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions)
        throws IOException {}

    @Override
    public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
      if (this.batchesProduced > this.batchesToProduce) {
        throw new NoSuchElementException();
      }

      if (this.batchesProduced == this.batchesToProduce) {
        this.batchesProduced++;
        return new SearchResultBatch(REMAINING_DOCS_LAST_BATCH).getBsonResults();
      }

      this.batchesProduced++;
      return new SearchResultBatch(MOCK_BATCH_SIZE).getBsonResults();
    }

    @Override
    public boolean isExhausted() {
      return this.batchesProduced > this.batchesToProduce;
    }

    @Override
    public void close() {}
  }

  public static SearchIndexReader.SearchProducerAndMetaResults mockSearchResultBatchProducer() {
    return mockSearchResultBatchProducer(Optional.empty());
  }

  static SearchIndexReader.SearchProducerAndMetaResults mockSearchResultBatchProducer(
      int numBatches) {
    return mockSearchResultBatchProducer(Optional.of(numBatches));
  }

  // TODO(CLOUDP-280897): Change the batchProducer concrete type to meet expectations for multi
  // index partitions.
  static SearchIndexReader.SearchProducerAndMetaResults mockSearchResultBatchProducer(
      Optional<Integer> numFullBatches) {
    return new SearchIndexReader.SearchProducerAndMetaResults(
        spy(new MockProducer(numFullBatches.orElse(2))),
        new MetaResults(CountResult.lowerBoundCount(getCount(numFullBatches.orElse(2)))));
  }

  static SearchIndexReader.SearchProducerAndMetaProducer mockIntermediateSearchResultBatchProducer(
      Optional<Integer> numFullBatches) {
    return new SearchIndexReader.SearchProducerAndMetaProducer(
        spy(new MockProducer(numFullBatches.orElse(2))),
        new CountMetaBatchProducer(getCount(numFullBatches.orElse(2))));
  }

  static SearchIndexReader.SearchProducerAndMetaProducer mockIntermediateSearchResultBatchProducer(
      Optional<Integer> numFullBatches, Optional<Integer> numMetaBatches) {
    return new SearchIndexReader.SearchProducerAndMetaProducer(
        spy(new MockProducer(numFullBatches.orElse(2))),
        spy(new MockProducer(numFullBatches.orElse(2))));
  }

  private static int getCount(int numFullBatches) {
    return numFullBatches * MOCK_BATCH_SIZE + REMAINING_DOCS_LAST_BATCH;
  }
}
