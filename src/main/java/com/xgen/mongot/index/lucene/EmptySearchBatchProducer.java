package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.util.Bytes;
import java.io.IOException;
import java.util.Optional;
import org.bson.BsonArray;

/**
 * EmptyBatchProducer implements BatchProducer, returning an empty set of results whenever
 * getNextBatch() is called.
 *
 * <p>This is useful for example when creating a cursor that should return no results.
 */
public class EmptySearchBatchProducer implements BatchProducer {
  private final Optional<LuceneIndexSearcherReference> searcherReference;
  private boolean isClosed;

  public EmptySearchBatchProducer(Optional<LuceneIndexSearcherReference> searcherReference) {
    this.searcherReference = searcherReference;
    this.isClosed = false;
  }

  @VisibleForTesting
  public EmptySearchBatchProducer() {
    this(Optional.empty());
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    checkState(!this.isClosed, "cannot call execute() after close()");
  }

  @Override
  public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
    checkState(!this.isClosed, "cannot call getNextBatch() after close()");
    return new BsonArray();
  }

  @Override
  public boolean isExhausted() {
    return true;
  }

  public MetaResults getMetaResults() {
    return MetaResults.EMPTY;
  }

  @Override
  public void close() throws IOException {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;
    if (this.searcherReference.isPresent()) {
      this.searcherReference.get().close();
    }
  }
}
