package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Unlike LuceneSearchBatchProducer, this class must start with all results pre-fetched, since
 * vector search does not support efficient pagination. Serialization into BSON batches is done
 * lazily as batches are requested.
 */
public class LuceneVectorSearchBatchProducer implements BatchProducer {

  private final List<VectorSearchResult> allResults;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;
  private final Optional<LuceneIndexSearcherReference> searcherReference;

  private final BatchSizeStrategy batchSizeStrategy;

  @Var private int nextResultIndex = 0;
  @Var private int nextBatchDocumentMax = BatchSizeStrategy.MAXIMUM_BATCH_SIZE;
  @Var private boolean closed = false;

  LuceneVectorSearchBatchProducer(
      List<VectorSearchResult> allResults,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      LuceneIndexSearcherReference searcherReference,
      BatchSizeStrategy batchSizeStrategy) {
    this.allResults = allResults;
    this.metricsUpdater = metricsUpdater;
    this.searcherReference = Optional.of(searcherReference);
    this.batchSizeStrategy = batchSizeStrategy;
  }

  LuceneVectorSearchBatchProducer(
      List<VectorSearchResult> allResults,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      BatchSizeStrategy batchSizeStrategy) {
    this.allResults = allResults;
    this.metricsUpdater = metricsUpdater;
    this.searcherReference = Optional.empty();
    this.batchSizeStrategy = batchSizeStrategy;
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    // All results are pre-fetched for vector queries but those results could be large so we still
    // follow cursor semantics for returning those results to the client.
    this.nextBatchDocumentMax = this.batchSizeStrategy.adviseNextBatchSize();
  }

  @Override
  public boolean isExhausted() {
    return this.nextResultIndex >= this.allResults.size();
  }

  @Override
  public final BsonArray getNextBatch(Bytes sizeLimit) throws IOException {
    if (this.closed) {
      throw new IOException(
          "internal error: attempted getNextBatch on closed LuceneVectorSearchBatchProducer");
    }

    try (var span = Tracing.detailedSpanGuard("mongot.lucene.vector_serialize_batch")) {
      var builder = BsonArrayBuilder.withLimit(sizeLimit);

      while (this.nextResultIndex < this.allResults.size()
          && builder.getDocumentCount() < this.nextBatchDocumentMax) {
        VectorSearchResult result = this.allResults.get(this.nextResultIndex);
        RawBsonDocument resultBson = result.toRawBson();

        if (!builder.append(resultBson)) {
          break; // The next result will be left for the next batch.
        }

        ++this.nextResultIndex;
      }

      var batch = builder.build();

      this.metricsUpdater.getBatchDocumentCount().record(builder.getDocumentCount());
      this.metricsUpdater.getBatchDataSize().record(builder.getDataSize().toBytes());
      span.getSpan().setAttribute("mongot.batch.result.count", batch.size());
      span.getSpan().setAttribute("mongot.batch.data_size.bytes", builder.getDataSize().toBytes());
      span.getSpan().setAttribute("mongot.vector.total_prefetched_results", this.allResults.size());
      addResultSamples(span.getSpan(), batch);

      checkState(
          this.nextResultIndex >= this.allResults.size() || builder.getDocumentCount() > 0,
          "Search result output exceeds BSON size limit");

      return batch;
    }
  }

  private static void addResultSamples(io.opentelemetry.api.trace.Span span, BsonArray results) {
    int sampleCount = Math.min(results.size(), Tracing.tracePayloadSampleDocs());
    span.setAttribute("mongot.result.sample.count", sampleCount);
    for (int i = 0; i < sampleCount; i++) {
      BsonValue value = results.get(i);
      String sample = value.isDocument() ? value.asDocument().toJson() : value.toString();
      Tracing.setPayloadAttribute(span, "mongot.result.sample." + i, sample);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }

    if (this.searcherReference.isPresent()) {
      this.searcherReference.get().close();
    }
    this.closed = true;
  }
}
