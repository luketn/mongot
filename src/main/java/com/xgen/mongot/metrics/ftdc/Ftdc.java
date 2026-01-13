package com.xgen.mongot.metrics.ftdc;

import java.io.IOException;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Records metrics in FTDC format.
 *
 * <p>Note: This class isn't thread safe. and assumes only one call at a time.
 */
public class Ftdc {
  private final FtdcConfig config;
  private final FtdcCollector collector;
  private final FtdcFileManager fileManager;

  // used for tests only
  Ftdc(FtdcConfig config, FtdcFileManager fileManager, FtdcCollector collector) {
    this.config = config;
    this.fileManager = fileManager;
    this.collector = collector;
  }

  /**
   * initialize Ftdc for this config, may throw IOException do if maintenance fails for the FTDC
   * directory.
   */
  public static Ftdc initialize(FtdcConfig config, FtdcMetadata metadata) throws IOException {
    FtdcFileManager fileManager = FtdcFileManager.initialize(config, metadata);
    FtdcCollector collector = new FtdcCollector();
    return new Ftdc(config, fileManager, collector);
  }

  /**
   * Records a sample, may or may not flush the sample to disk in accordance with the configuration.
   * If this sample changes the schema from the previously recorded samples, a flush will occur.
   */
  void addSample(BsonDocument sample, long epochTime) throws IOException {
    Optional<MetricChunk> chunkToFlush = this.collector.collect(sample, epochTime);

    if (chunkToFlush.isPresent()) {
      // Collector detected a schema change.
      // We only keep one schema in memory at a time, so we flush this chunk to the archive files in
      // favor of the new one. (chunkToFlush is already cleared from the collector. no need to clear
      // because the collector is only buffering the current schema).
      flushToArchive(chunkToFlush.get());

    } else if (needToFlushArchive()) {
      // The archive files are the permanent storage of FTDC, once we flush a metric chunk to it, we
      // will start collecting a new one. We can clear any interim data from disk: the
      // interim data is strictly contained in the current chunk being written.
      flushToArchiveAndClear();

    } else if (needToFlushInterim()) {
      // We use the interim files to frequently overwrite the most recent data in case we shutdown.
      // The interim file only contains the last chunk that hasn't been archived yet.
      flushToInterim();
    }
  }

  private boolean needToFlushArchive() {
    return this.collector.getNumSamples() >= this.config.samplesPerMetricChunk;
  }

  private void flushToArchiveAndClear() throws IOException {
    Optional<MetricChunk> chunk = this.collector.getCurrentChunk();
    // start a new metric chunk after this one is written
    this.collector.clear();

    if (chunk.isPresent()) {
      flushToArchive(chunk.get());
    }
  }

  private boolean needToFlushInterim() {
    int samples = this.collector.getNumSamples();

    return samples > 0 && (samples % this.config.samplesPerInterimUpdate == 0);
  }

  private void flushToInterim() throws IOException {
    Optional<MetricChunk> chunk = this.collector.getCurrentChunk();
    // We do not clear the collector, we do so only on an archive flush.

    if (chunk.isPresent()) {
      flushToInterim(chunk.get());
    }
  }

  private void flushToInterim(MetricChunk metricChunk) throws IOException {
    BsonDocument chunk = FtdcCompressor.compressChunk(metricChunk);
    try {
      this.fileManager.replaceInterim(chunk);
    } catch (IOException e) {
      // clear the buffered data before rethrowing, otherwise, we will leak data if the exception
      // is ignored upstream.
      this.collector.clear();
      throw e;
    }
  }

  private void flushToArchive(MetricChunk metricChunk) throws IOException {
    BsonDocument chunkDocument = FtdcCompressor.compressChunk(metricChunk);
    try {
      this.fileManager.writeAndClearInterim(chunkDocument);
    } catch (IOException e) {
      // clear the buffered data before rethrowing, otherwise, we will leak data if the exception
      // is ignored upstream.
      this.collector.clear();
      throw e;
    }
  }
}
