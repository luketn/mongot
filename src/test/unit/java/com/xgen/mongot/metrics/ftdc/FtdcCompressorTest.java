package com.xgen.mongot.metrics.ftdc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.junit.Assert;
import org.junit.Test;

public class FtdcCompressorTest {
  @Test
  public void testEmptyMetricChunkCanNotBeInstantiated() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new MetricChunk(new BsonDocument(), new LinkedHashMap<>(), 42));
  }

  @Test
  public void testMetricChunkWithoutSamplesCanNotBeInstantiated() {
    LinkedHashMap<String, List<Long>> emptySamples = new LinkedHashMap<>();
    emptySamples.put("foo", Collections.emptyList());
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new MetricChunk(new BsonDocument("foo", new BsonInt32(3)), emptySamples, 42));
  }

  @Test
  public void testOneSampleCanBeCompressed() throws Exception {
    FtdcCollector collector = new FtdcCollector();
    addSampleAndRoundTrip(collector, 42, 123);
  }

  @Test
  public void testRoundTrip() throws Exception {
    FtdcCollector collector = new FtdcCollector();
    addSampleAndRoundTrip(collector, 1, 10);

    addSampleAndRoundTrip(collector, 2, 20);

    addSampleAndRoundTrip(collector, 3, 30);

    // numbers stay the same: tests for zero run encoding
    for (int i = 0; i < 10; i++) {
      addSampleAndRoundTrip(collector, 4, 40);
    }

    // test for decreasing numbers:
    addSampleAndRoundTrip(collector, 2, 2);

    // test negative samples
    addSampleAndRoundTrip(collector, -2, -2);
  }

  private void addSampleAndRoundTrip(FtdcCollector collector, int foo, int bar) throws Exception {
    BsonDocument sample =
        new BsonDocument("foo", new BsonInt32(foo)).append("bar", new BsonInt64(bar));
    collector.collect(sample, 42);
    MetricChunk chunk = collector.getCurrentChunk().orElseThrow();
    assertRoundTrips(chunk);
  }

  private void assertRoundTrips(MetricChunk chunk) throws Exception {
    var document = FtdcCompressor.compressChunk(chunk);
    var decoded = FtdcDecoder.decodeMetricChunk(document);
    Assert.assertEquals(chunk, decoded);
  }
}
