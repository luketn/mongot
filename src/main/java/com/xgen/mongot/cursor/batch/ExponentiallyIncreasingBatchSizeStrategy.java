package com.xgen.mongot.cursor.batch;

public class ExponentiallyIncreasingBatchSizeStrategy implements BatchSizeStrategy {

  /**
   * The power function is selected based on empirical observations and works for all target use
   * cases. For more information, see https://tinyurl.com/4fbbm4b7
   */
  private static final double POWER = 1.03;

  private int nextBatchSize = DEFAULT_BATCH_SIZE;

  @Override
  public int adviseNextBatchSize() {
    return this.nextBatchSize;
  }

  @Override
  public void adjust(BatchCursorOptions options) {
    // intValue() takes care of casting int to double, returning the largest int in case of overflow
    this.nextBatchSize = Double.valueOf(Math.pow(this.nextBatchSize, POWER)).intValue();
  }
}
