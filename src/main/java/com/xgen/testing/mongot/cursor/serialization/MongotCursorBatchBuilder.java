package com.xgen.testing.mongot.cursor.serialization;

import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.Optional;

public class MongotCursorBatchBuilder {
  private Optional<MongotCursorResult> cursor = Optional.empty();
  private Optional<SearchExplainInformation> explain = Optional.empty();
  private Optional<Variables> variables = Optional.empty();
  private int ok = 1;

  public static MongotCursorBatchBuilder builder() {
    return new MongotCursorBatchBuilder();
  }

  public MongotCursorBatchBuilder cursor(MongotCursorResult cursor) {
    this.cursor = Optional.of(cursor);
    return this;
  }

  public MongotCursorBatchBuilder explain(SearchExplainInformation explain) {
    this.explain = Optional.of(explain);
    return this;
  }

  public MongotCursorBatchBuilder variables(Variables variables) {
    this.variables = Optional.of(variables);
    return this;
  }

  public MongotCursorBatchBuilder ok(int ok) {
    this.ok = ok;
    return this;
  }

  public MongotCursorBatch build() {
    return new MongotCursorBatch(
        this.cursor, this.explain, this.variables.map(DocumentEncodable::toRawBson), this.ok);
  }
}
