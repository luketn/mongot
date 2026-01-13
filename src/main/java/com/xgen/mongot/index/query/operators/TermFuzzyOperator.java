package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record TermFuzzyOperator(
    Score score, List<StringPath> paths, List<String> query, FuzzyOption fuzzy)
    implements TermOperator {
  @Override
  public Type getType() {
    return Type.TERM_FUZZY;
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilderWithStringPath(this.score, this.paths, this.query)
        .field(Fields.FUZZY, Optional.of(this.fuzzy))
        .build();
  }

  public int maxEdits() {
    return this.fuzzy.maxEdits();
  }

  /*
   * If a prefixLength > 0 is specified, a common prefix of that length is required.
   */
  public int prefixLength() {
    return this.fuzzy.prefixLength();
  }

  public int maxExpansions() {
    return this.fuzzy.maxExpansions();
  }
}
