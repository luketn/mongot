package com.xgen.mongot.index;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;

/** This class is responsible for merging results from multiple CountMetaBatchProducers. */
public class CountMergingBatchProducer implements BatchProducer {

  private final List<CountMetaBatchProducer> batchProducers;

  private boolean countProduced;

  @Var private Optional<Long> totalCount;

  public CountMergingBatchProducer(List<CountMetaBatchProducer> batchProducers) {
    this.batchProducers = batchProducers;
    this.countProduced = false;
    this.totalCount = Optional.empty();
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    this.countProduced = true;
    this.totalCount =
        Optional.of(this.batchProducers.stream().mapToLong(CountMetaBatchProducer::getCount).sum());
  }

  @Override
  public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
    long count = Check.isPresent(this.totalCount, "totalCount");
    return CountMetaBatchProducer.getCountBatchResult(count);
  }

  @Override
  public boolean isExhausted() {
    return this.countProduced;
  }

  @Override
  public void close() {}
}
