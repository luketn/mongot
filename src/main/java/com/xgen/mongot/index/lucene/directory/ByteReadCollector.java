package com.xgen.mongot.index.lucene.directory;

import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tags;
import org.apache.lucene.store.DataInput;

/**
 * Collect byte reads data that are accessed by {@link FileSystemDirectory}, at file type level. For
 * now, we will only collect them during perf tests, due to the concern of concurrent accessing
 * atomic counter for each {@link DataInput#readByte()} and {@link DataInput#readBytes(byte[], int,
 * int)} call. This data will be used to track whether more reads are performed against certain file
 * types after new feature launches.
 *
 * <p>We are expecting, during the lifecycle of mongot process, there will only be one
 * ByteReadCollector instance (Same as {@link MeterAndFtdcRegistry} which is created in
 * MmsMongotBootstrapper#bootstrap(Path)), and it will be created by {@link
 * com.xgen.mongot.index.lucene.LuceneIndexFactory} and shared across all {@link
 * IndexDirectoryFactory} instances, thus all {@link FileSystemDirectory} instances.
 *
 * <p>We will consider presenting them via explain API in the future if no performance impact is
 * observed.
 */
public class ByteReadCollector {
  private final MetricsFactory metricsFactory;
  static final String METRIC_NAME = "directoryBytesRead";

  public ByteReadCollector(MetricsFactory metricsFactory) {
    this.metricsFactory = metricsFactory;
  }

  public void collect(String extension, int bytes) {
    this.metricsFactory.counter(METRIC_NAME, Tags.of("fileType", extension)).increment(bytes);
  }
}
