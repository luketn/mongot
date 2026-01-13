package com.xgen.mongot.cursor;

import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import java.util.Optional;
import org.bson.BsonValue;

/**
 * Holds a batch returned from a MongotCursor, along with info about the namespace the batch was
 * produced for and whether there are subsequent batches.
 */
public class MongotCursorResultInfo {

  public final boolean exhausted;
  public final BsonValue batch;
  public final Optional<SearchExplainInformation> explainResult;
  public final String namespace;

  public MongotCursorResultInfo(boolean exhausted, BsonValue batch, String namespace) {
    this(exhausted, batch, Optional.empty(), namespace);
  }

  public MongotCursorResultInfo(
      boolean exhausted,
      BsonValue batch,
      Optional<SearchExplainInformation> explainResult,
      String namespace) {
    this.exhausted = exhausted;
    this.batch = batch;
    this.explainResult = explainResult;
    this.namespace = namespace;
  }

  public MongotCursorResult toCursorResult(long cursorId, Optional<MongotCursorResult.Type> type) {
    return new MongotCursorResult(
        this.exhausted ? MongotCursorResult.EXHAUSTED_CURSOR_ID : cursorId,
        this.batch,
        this.namespace,
        type);
  }

  public MongotCursorResult toCursorResult(long cursorId) {
    return toCursorResult(cursorId, Optional.empty());
  }
}
