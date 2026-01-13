package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import java.util.Optional;

public abstract class OperatorBuilder<T, B extends OperatorBuilder<T, B>> {

  public static AutocompleteOperatorBuilder autocomplete() {
    return new AutocompleteOperatorBuilder();
  }

  public static CompoundOperatorBuilder compound() {
    return new CompoundOperatorBuilder();
  }

  public static EmbeddedDocumentOperatorBuilder embeddedDocument() {
    return new EmbeddedDocumentOperatorBuilder();
  }

  public static EqualsOperatorBuilder equals() {
    return new EqualsOperatorBuilder();
  }

  public static ExistsOperatorBuilder exists() {
    return new ExistsOperatorBuilder();
  }

  public static HasAncestorOperatorBuilder hasAncestor() {
    return new HasAncestorOperatorBuilder();
  }

  public static HasRootOperatorBuilder hasRoot() {
    return new HasRootOperatorBuilder();
  }

  public static InOperatorBuilder in() {
    return new InOperatorBuilder();
  }

  public static KnnBetaOperatorBuilder knnBeta() {
    return new KnnBetaOperatorBuilder();
  }

  public static NearOperatorBuilder near() {
    return new NearOperatorBuilder();
  }

  public static MoreLikeThisOperatorBuilder moreLikeThis() {
    return new MoreLikeThisOperatorBuilder();
  }

  public static PhraseOperatorBuilder phrase() {
    return new PhraseOperatorBuilder();
  }

  public static QueryStringOperatorBuilder queryString() {
    return new QueryStringOperatorBuilder();
  }

  public static RangeOperatorBuilder range() {
    return new RangeOperatorBuilder();
  }

  public static RegexOperatorBuilder regex() {
    return new RegexOperatorBuilder();
  }

  public static SearchOperatorBuilder search() {
    return new SearchOperatorBuilder();
  }

  public static TermOperatorBuilder term() {
    return new TermOperatorBuilder();
  }

  public static TextOperatorBuilder text() {
    return new TextOperatorBuilder();
  }

  public static WildcardOperatorBuilder wildcard() {
    return new WildcardOperatorBuilder();
  }

  public static GeoWithinOperatorBuilder geoWithin() {
    return new GeoWithinOperatorBuilder();
  }

  public static GeoShapeOperatorBuilder geoShape() {
    return new GeoShapeOperatorBuilder();
  }

  public static VectorSearchOperatorBuilder vectorSearch() {
    return new VectorSearchOperatorBuilder();
  }

  private Optional<Score> score = Optional.empty();

  public abstract T build();

  abstract B getBuilder();

  public B score(Score score) {
    this.score = Optional.of(score);
    return getBuilder();
  }

  Score getScore() {
    return this.score.orElseGet(Score::defaultScore);
  }
}
