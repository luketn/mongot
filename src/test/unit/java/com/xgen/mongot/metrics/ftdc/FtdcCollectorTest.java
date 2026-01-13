package com.xgen.mongot.metrics.ftdc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FtdcCollectorTest {
  private static final int EPOCH_TIME = 42;
  private FtdcCollector collector;

  @Before
  public void setUp() {
    this.collector = new FtdcCollector();
  }

  @Test
  public void testNothingCollectedNoChunk() {
    Assert.assertEquals(0, this.collector.getNumSamples());
    Assert.assertTrue(this.collector.getCurrentChunk().isEmpty());
  }

  @Test
  public void testFirstSampleCollectedDoesNotNeedFlush() {
    var needsFlush = addSample(docWithFoo(4));
    Assert.assertTrue(needsFlush.isEmpty());
  }

  @Test
  public void testEmptySamplesThrowsError() {
    Assert.assertThrows(IllegalArgumentException.class, () -> addSample(new BsonDocument()));
  }

  @Test
  public void testNonNumericMetricsIgnored() {
    BsonDocument doc1 = docWithFoo(5);
    BsonDocument doc2 = docWithFoo(5).append("ignoredd", new BsonString("ignored"));
    addSample(doc1);
    addSample(doc2);
    Assert.assertEquals(2, this.collector.getNumSamples());
  }

  @Test
  public void testSameSchemaAppendsSamples() {
    BsonDocument firstDocument = docWithFoo(4);
    addSample(firstDocument);
    addSample(docWithFoo(5));

    var chunk = getCurrentChunk();

    Assert.assertEquals(Map.of("foo", List.of(4L, 5L)), chunk.metrics());
    Assert.assertEquals(firstDocument, chunk.schema());
    Assert.assertEquals(EPOCH_TIME, chunk.epochTime());
  }

  /**
   * Test that the order of metrics is taken from the first sample. The order of metrics in
   * subsequent samples does not matter (only thing that matters is whether the schema has changed).
   */
  @Test
  public void testOrderOfSamplesDoesNotMatter() {
    BsonDocument firstDocument =
        new BsonDocument().append("foo", new BsonInt32(4)).append("bar", new BsonInt32(30));
    addSample(firstDocument);
    addSample(new BsonDocument().append("bar", new BsonInt32(31)).append("foo", new BsonInt32(5)));

    var chunk = getCurrentChunk();

    Assert.assertEquals(Map.of("foo", List.of(4L, 5L), "bar", List.of(30L, 31L)), chunk.metrics());
    Assert.assertEquals(firstDocument, chunk.schema());
    Assert.assertEquals(EPOCH_TIME, chunk.epochTime());
  }

  @Test
  public void testChunkTimeIsFirstSampleTime() {
    this.collector.collect(docWithFoo(4), 42);
    this.collector.collect(docWithFoo(5), 50);

    var chunk = getCurrentChunk();

    Assert.assertEquals(42, chunk.epochTime());
  }

  /**
   * We we add a sample with a new schema, we expect the collector to return the previous chunk, so
   * we can flush it.
   */
  @Test
  public void testSchemaChangesOnCollectNeedsFlush() {
    BsonDocument firstDocument = docWithFoo(4);
    BsonDocument secondDocument = docWithBar(5);

    addSample(firstDocument);
    var chunk =
        addSample(secondDocument)
            .orElseThrow(() -> new AssertionError("expected present chunk with old schema"));

    Assert.assertEquals(Map.of("foo", List.of(4L)), chunk.metrics());
    Assert.assertEquals(firstDocument, chunk.schema());

    MetricChunk secondChunk = getCurrentChunk();
    Assert.assertEquals(secondDocument, secondChunk.schema());
  }

  @Test
  public void testGetNumSamples() {
    Assert.assertEquals(0, this.collector.getNumSamples());

    addSample(docWithFoo(0));
    Assert.assertEquals(1, this.collector.getNumSamples());

    addSample(docWithFoo(0));
    Assert.assertEquals(2, this.collector.getNumSamples());

    addSample(docWithBar(0));
    // schema has changed now we have 1 sample
    Assert.assertEquals(1, this.collector.getNumSamples());

    this.collector.clear();
    Assert.assertEquals(0, this.collector.getNumSamples());
  }

  @Test
  public void testResetCleansCurrentChunk() {
    addSample(docWithFoo(4));
    var chunk = this.collector.getCurrentChunk();
    Assert.assertTrue(chunk.isPresent());

    this.collector.clear();

    var nothingToFlush = this.collector.getCurrentChunk();
    Assert.assertTrue(nothingToFlush.isEmpty());
  }

  @Test
  public void testExtractedValuesForDataTypes() {
    var doc =
        new BsonDocument()
            .append("bool true", BsonBoolean.TRUE)
            .append("bool false", BsonBoolean.FALSE)
            .append("int64", new BsonInt64(64))
            .append("int32", new BsonInt64(32))
            .append("-int64", new BsonInt64(-64))
            .append("-int32", new BsonInt64(-32))
            .append("double", new BsonDouble(3.1415))
            .append("-double", new BsonDouble(-3.1415))
            .append("double > 2^63 truncated to max long", new BsonDouble(2E65))
            .append("double < -2^63 truncated to min long", new BsonDouble(-3.1415E65))
            .append("small double rounded to 0", new BsonDouble(1E-10))
            .append("small double rounded to 1", new BsonDouble(1 + 1E-10))
            .append("small negative double rounded to 0", new BsonDouble(1E-10))
            .append("datetime", new BsonDateTime(2020))
            .append("timestamp", new BsonTimestamp(123456789))
            .append("string ignored", new BsonString("foo"))
            .append(
                "array",
                new BsonArray(List.of(BsonBoolean.TRUE, new BsonInt64(64), new BsonString("foo"))));

    addSample(doc);

    var samples = getCurrentChunk().metrics();
    assertSampleValue(samples, "bool true", 1);
    assertSampleValue(samples, "bool false", 0);
    assertSampleValue(samples, "int64", 64L);
    assertSampleValue(samples, "int32", 32L);
    assertSampleValue(samples, "-int64", -64L);
    assertSampleValue(samples, "-int32", -32L);
    assertSampleValue(samples, "double", 3); // doubles get truncated
    assertSampleValue(samples, "-double", -3);
    assertSampleValue(samples, "double > 2^63 truncated to max long", Long.MAX_VALUE);
    assertSampleValue(samples, "double < -2^63 truncated to min long", Long.MIN_VALUE);
    assertSampleValue(samples, "small double rounded to 0", 0);
    assertSampleValue(samples, "small double rounded to 1", 1);
    assertSampleValue(samples, "small negative double rounded to 0", 0);
    assertSampleValue(samples, "datetime", 2020);
    assertSampleValue(samples, "timestamp", 0);
    assertSampleValue(samples, "timestamp.inc", 123456789);
    assertSampleValue(samples, "array.1", 1);
    assertSampleValue(samples, "array.2", 64);
    Assert.assertFalse(samples.containsKey("array.3"));
    Assert.assertFalse(samples.containsKey("string ignored"));
  }

  @Test
  public void testNestedDocuments() {
    var doc =
        new BsonDocument()
            .append("bool", BsonBoolean.TRUE)
            .append(
                "level1",
                new BsonDocument()
                    .append("datetime", new BsonDateTime(2020))
                    .append("int32", new BsonInt32(32))
                    .append(
                        "level2",
                        new BsonDocument()
                            .append("double", new BsonDouble(3.1415))
                            .append(
                                "array",
                                new BsonArray(
                                    List.of(
                                        new BsonDocument().append("int64", new BsonInt64(64)))))));

    addSample(doc);

    var samples = getCurrentChunk().metrics();

    assertSampleValue(samples, "bool", 1);
    assertSampleValue(samples, "level1.datetime", 2020);
    assertSampleValue(samples, "level1.int32", 32L);
    assertSampleValue(samples, "level1.level2.double", 3);
    assertSampleValue(samples, "level1.level2.array.1.int64", 64);
  }

  @Test
  public void testArrays() {
    var doc =
        new BsonDocument()
            .append("bool", BsonBoolean.TRUE)
            .append(
                "array",
                new BsonArray(
                    List.of(
                        new BsonDateTime(2020),
                        new BsonInt64(32),
                        new BsonDocument().append("double", new BsonDouble(3.1415)))));

    addSample(doc);

    var samples = getCurrentChunk().metrics();

    assertSampleValue(samples, "bool", 1);
    assertSampleValue(samples, "array.1", 2020);
    assertSampleValue(samples, "array.2", 32);
    assertSampleValue(samples, "array.3.double", 3);
  }

  private void assertSampleValue(
      LinkedHashMap<String, List<Long>> samples, String name, long value) {
    Assert.assertEquals(List.of(value), samples.get(name));
  }

  private MetricChunk getCurrentChunk() {
    return this.collector
        .getCurrentChunk()
        .orElseThrow(() -> new AssertionError("expected chunk to be present"));
  }

  private Optional<MetricChunk> addSample(BsonDocument sample) {
    return this.collector.collect(sample, EPOCH_TIME);
  }

  private BsonDocument docWithFoo(int value) {
    return new BsonDocument().append("foo", new BsonInt64(value));
  }

  private BsonDocument docWithBar(int value) {
    return new BsonDocument().append("bar", new BsonInt64(value));
  }
}
