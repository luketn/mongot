package com.xgen.mongot.cursor.batch;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class AdjustableBatchSizeStrategyTest {

  private static final int DOCS_REQUESTED = 25;
  private static final int DOCS_EXPECTED = 27;

  @Test
  public void testSimple() {
    var options = new BatchCursorOptions(Optional.of(DOCS_REQUESTED), Optional.empty());
    var strategy = AdjustableBatchSizeStrategy.create(options, false);
    Assert.assertEquals(DOCS_EXPECTED, strategy.adviseNextBatchSize());
  }

  @Test
  public void testSimpleBatchSize() {
    var options = new BatchCursorOptions(Optional.empty(), Optional.of(DOCS_REQUESTED));
    var strategy = AdjustableBatchSizeStrategy.create(options, false);
    Assert.assertEquals(DOCS_REQUESTED, strategy.adviseNextBatchSize());
  }

  @Test
  public void testStoredSource() {
    var options = new BatchCursorOptions(Optional.of(DOCS_REQUESTED), Optional.empty());
    var strategy = AdjustableBatchSizeStrategy.create(options, true);
    Assert.assertEquals(DOCS_REQUESTED, strategy.adviseNextBatchSize());
  }

  @Test
  public void testTooLargeLimit() {
    var options = new BatchCursorOptions(Optional.of(10_000_000), Optional.empty());
    var strategy = AdjustableBatchSizeStrategy.create(options, false);
    Assert.assertEquals(BatchSizeStrategy.MAXIMUM_BATCH_SIZE, strategy.adviseNextBatchSize());
  }

  @Test
  public void testTooSmallLimit() {
    var options = new BatchCursorOptions(Optional.of(0), Optional.empty());
    var strategy = AdjustableBatchSizeStrategy.create(options, false);
    Assert.assertEquals(BatchSizeStrategy.MINIMUM_BATCH_SIZE, strategy.adviseNextBatchSize());
  }

  @Test
  public void testAdjust() {
    var strategy = new AdjustableBatchSizeStrategy(false);
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
    var options = new BatchCursorOptions(Optional.of(DOCS_REQUESTED), Optional.empty());
    strategy.adjust(options);
    Assert.assertEquals(DOCS_EXPECTED, strategy.adviseNextBatchSize());
  }

  @Test
  public void testAdjustLegacy() {
    var strategy = new AdjustableBatchSizeStrategy(false);
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
    var options = new BatchCursorOptions(Optional.empty(), Optional.empty());
    strategy.adjust(options);
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
  }

  @Test
  public void testAdjustBatchSize() {
    var strategy = new AdjustableBatchSizeStrategy(false);
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
    var options = new BatchCursorOptions(Optional.empty(), Optional.of(DOCS_REQUESTED));
    strategy.adjust(options);
    Assert.assertEquals(DOCS_REQUESTED, strategy.adviseNextBatchSize());
  }

  @Test
  public void testCreateOptionalEmpty() {
    var options = new BatchCursorOptions(Optional.empty(), Optional.empty());
    var strategy = AdjustableBatchSizeStrategy.create(options, false);
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
  }

  @Test
  public void testAdviseTwice() {
    var strategy = new AdjustableBatchSizeStrategy(false);
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.DEFAULT_BATCH_SIZE, strategy.adviseNextBatchSize());
  }

  @Test
  public void testBothSettingsNotAllowed() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new BatchCursorOptions(Optional.of(DOCS_REQUESTED), Optional.of(DOCS_REQUESTED)));
  }
}
