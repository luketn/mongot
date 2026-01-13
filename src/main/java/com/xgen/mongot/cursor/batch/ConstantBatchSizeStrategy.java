package com.xgen.mongot.cursor.batch;

public class ConstantBatchSizeStrategy implements BatchSizeStrategy {

  private final int size;

  public ConstantBatchSizeStrategy() {
    this.size = DEFAULT_BATCH_SIZE;
  }

  public ConstantBatchSizeStrategy(int size) {
    this.size = size;
  }

  @Override
  public int adviseNextBatchSize() {
    return this.size;
  }

  @Override
  public void adjust(BatchCursorOptions options) {
    // noop
  }
}
