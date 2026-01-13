package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record SearchPhrasePrefixOperator(
    SearchDisjunctionOperator baseOperator, PhraseOption phraseOption) implements SearchOperator {
  @Override
  public Type getType() {
    return Type.SEARCH_PHRASE_PREFIX;
  }

  @Override
  public Score score() {
    return this.baseOperator.score();
  }

  @Override
  public List<StringPath> paths() {
    return this.baseOperator.paths();
  }

  @Override
  public List<String> query() {
    return this.baseOperator.query();
  }

  @Override
  public BsonValue operatorToBson() {
    return this.baseOperator.toBuilder()
        .field(Fields.PHRASE, Optional.of(this.phraseOption))
        .build();
  }

  public int slop() {
    return this.phraseOption.slop();
  }

  public int maxExpansions() {
    return this.phraseOption.maxExpansions();
  }
}
