package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record SearchPhraseOperator(
    SearchDisjunctionOperator searchDisjunctionOperator, PhraseOption phraseOption)
    implements SearchOperator {

  @Override
  public Type getType() {
    return Type.SEARCH_PHRASE;
  }

  @Override
  public Score score() {
    return this.searchDisjunctionOperator.score();
  }

  @Override
  public List<StringPath> paths() {
    return this.searchDisjunctionOperator.paths();
  }

  @Override
  public List<String> query() {
    return this.searchDisjunctionOperator.query();
  }

  @Override
  public BsonValue operatorToBson() {
    return this.searchDisjunctionOperator.toBuilder()
        .field(Fields.PHRASE, Optional.of(this.phraseOption))
        .build();
  }

  public int slop() {
    return this.phraseOption.slop();
  }
}
