package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import java.util.List;
import org.bson.BsonValue;

public record TermPrefixOperator(Score score, List<StringPath> paths, List<String> query)
    implements TermOperator {
  @Override
  public Type getType() {
    return Type.TERM_PREFIX;
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilderWithStringPath(this.score, this.paths, this.query)
        .field(Fields.PREFIX, true)
        .build();
  }
}
