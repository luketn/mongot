package com.xgen.mongot.index;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.util.Bytes;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonArray;

public class MeteredBatchProducer implements BatchProducer {
  private final BatchProducer batchProducer;
  private final IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater;
  private final AtomicInteger counter;

  public MeteredBatchProducer(
      BatchProducer batchProducer,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater) {
    this.batchProducer = batchProducer;
    this.queryingMetricsUpdater = queryingMetricsUpdater;
    this.counter = new AtomicInteger();
  }

  public BatchProducer unwrap() {
    return this.batchProducer;
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    try {
      this.counter.incrementAndGet();
      this.batchProducer.execute(sizeLimit, queryCursorOptions);
    } catch (Exception e) {
      // Don't know the original query, so put a "from BatchProducer" here.
      this.queryingMetricsUpdater.handleQueryException(e, "from BatchProducer");
      throw e;
    }
  }

  @Override
  public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
    return this.batchProducer.getNextBatch(resultsSizeLimit);
  }

  @Override
  public boolean isExhausted() {
    return this.batchProducer.isExhausted();
  }

  @Override
  public void close() throws IOException {
    this.queryingMetricsUpdater.getSearchAndGetMoreCommandPerQuery().record(this.counter.get());
    this.batchProducer.close();
  }
}
