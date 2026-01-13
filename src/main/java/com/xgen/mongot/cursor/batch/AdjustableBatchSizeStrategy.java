package com.xgen.mongot.cursor.batch;

import com.google.common.primitives.Ints;

public class AdjustableBatchSizeStrategy implements BatchSizeStrategy {

  /**
   * Oversubscription coefficient for extractable limit queries was determined heuristically based
   * on the number of orphaned and deleted documents that mongot may be behind on. Full details in
   * document: https://tinyurl.com/y5b649a8
   */
  @Deprecated(since = "MongoDB 8.1", forRemoval = true)
  public static final double OVER_SUBSCRIPTION = 1.064;

  private int nextBatchSize;
  private final boolean returnStoredSource;

  public AdjustableBatchSizeStrategy(boolean returnStoredSource) {
    this.nextBatchSize = DEFAULT_BATCH_SIZE;
    this.returnStoredSource = returnStoredSource;
  }

  public AdjustableBatchSizeStrategy(int nextBatchSize, boolean returnStoredSource) {
    this.nextBatchSize = nextBatchSize;
    this.returnStoredSource = returnStoredSource;
  }

  public static AdjustableBatchSizeStrategy create(
      BatchCursorOptions options, boolean returnStoredSource) {
    return new AdjustableBatchSizeStrategy(
        computeNextBatchSize(options, DEFAULT_BATCH_SIZE, returnStoredSource), returnStoredSource);
  }

  public void adjust(BatchCursorOptions options) {
    this.nextBatchSize = computeNextBatchSize(options, this.nextBatchSize, this.returnStoredSource);
  }

  @SuppressWarnings({"removal"})
  private static int computeNextBatchSize(
      BatchCursorOptions options, int nextBatchSize, boolean returnStoredSource) {
    if (options.getDocsRequested().isPresent()) {
      return handleDocsRequested(options.getDocsRequested().get(), returnStoredSource);
    } else if (options.getBatchSize().isPresent()) {
      return options.getBatchSize().get();
    } else {
      return nextBatchSize;
    }
  }

  @SuppressWarnings({"removal"})
  @Deprecated(since = "MongoDB 8.1", forRemoval = true)
  private static int handleDocsRequested(int docsRequested, boolean returnStoredSource) {
    // There's no threat of orphaned documents/deleted docs behind with stored source, so
    // no need to oversubscribe
    if (returnStoredSource) {
      return Ints.constrainToRange(docsRequested, MINIMUM_BATCH_SIZE, MAXIMUM_BATCH_SIZE);
    }
    return Ints.constrainToRange(
        (int) Math.ceil(OVER_SUBSCRIPTION * docsRequested), MINIMUM_BATCH_SIZE, MAXIMUM_BATCH_SIZE);
  }

  @Override
  public int adviseNextBatchSize() {
    return this.nextBatchSize;
  }
}
