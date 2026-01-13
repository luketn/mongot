package com.xgen.testing.mongot.cursor.serialization;

import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotIntermediateCursorBatch;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class MongotIntermediateCursorBatchBuilder {
  private Optional<MongotCursorBatch> metaCursor = Optional.empty();
  private Optional<MongotCursorBatch> searchCursor = Optional.empty();
  private int ok = 1;

  public static MongotIntermediateCursorBatchBuilder builder() {
    return new MongotIntermediateCursorBatchBuilder();
  }

  public MongotIntermediateCursorBatchBuilder metaCursor(MongotCursorBatch metaCursor) {
    this.metaCursor = Optional.of(metaCursor);
    return this;
  }

  public MongotIntermediateCursorBatchBuilder searchCursor(MongotCursorBatch searchCursor) {
    this.searchCursor = Optional.of(searchCursor);
    return this;
  }

  public MongotIntermediateCursorBatchBuilder ok(int ok) {
    this.ok = ok;
    return this;
  }

  public MongotIntermediateCursorBatch build() {
    Check.isPresent(this.metaCursor, "metaCursor");
    Check.isPresent(this.searchCursor, "searchCursor");
    return new MongotIntermediateCursorBatch(
        this.metaCursor.get(), this.searchCursor.get(), this.ok);
  }
}
