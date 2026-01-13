package com.xgen.mongot.index;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.util.Bytes;
import java.io.IOException;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;

/**
 * CountMetaBatchProducer is responsible for outputting intermediate meta results for a query that
 * only returns count information for metadata. It simply outputs a single bucket of type "count"
 * with the count of documents returned by the search.
 *
 * <pre>
 *   {
 *     "type": "count",
 *     "count": 5
 *   }
 * </pre>
 */
public class CountMetaBatchProducer implements BatchProducer {

  private final long count;

  private boolean countProduced;

  public CountMetaBatchProducer(long count) {
    this.count = count;
    this.countProduced = false;
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    this.countProduced = true;
  }

  @Override
  public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
    return getCountBatchResult(this.count);
  }

  @Override
  public boolean isExhausted() {
    return this.countProduced;
  }

  @Override
  public void close() {}

  public long getCount() {
    return this.count;
  }

  static BsonArray getCountBatchResult(long count) {
    return new BsonArray(
        List.of(
            new BsonDocument("type", new BsonString("count"))
                .append("count", new BsonInt64(count))));
  }
}
