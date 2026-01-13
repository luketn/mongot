package com.xgen.mongot.cursor;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Range;

class CursorIdSupplier {

  private final Range<Long> idRange;
  private final AtomicLong allocator;

  private CursorIdSupplier(Range<Long> idRange, long initialValue) {
    this.idRange = idRange;
    this.allocator = new AtomicLong(initialValue);
  }

  static CursorIdSupplier fromRange(Range<Long> idRange) {
    // We start the initial value for the supplier at a random number in the range of allowed
    // values. We do this to attempt to prevent the following scenario:
    //  - mongot starts up
    //  - user issues query Q1, mongot returns cursor id C1 to mongod
    //  - mongot restarts
    //  - user issues a second, different query Q2, mongot again returns cursor id C1 to mongod
    //  - mongod issues a getMore on the cursor for Q1, unaware mongot has restarted, and gets
    //    semantically invalid results due to the overlapping cursors
    //
    // This scenario can theoretically happen any time mongot restarts since we do not persist open
    // cursor ids. Starting the initial value of the supplier at a random value each time should
    // give us decent protection against overlapping cursor ids.
    long initialValue =
        ThreadLocalRandom.current().nextLong(idRange.getMinimum(), idRange.getMaximum());
    return new CursorIdSupplier(idRange, initialValue);
  }

  @VisibleForTesting
  static CursorIdSupplier createDefault() {
    return fromRange(CursorConfig.DEFAULT_CURSOR_ID_RANGE);
  }

  long nextId() {
    return this.allocator.updateAndGet(
        lastId -> {
          var nextId = lastId + 1;
          // Ensure we wrap around to the minimum of our range.
          return this.idRange.contains(nextId) ? nextId : this.idRange.getMinimum();
        });
  }
}
