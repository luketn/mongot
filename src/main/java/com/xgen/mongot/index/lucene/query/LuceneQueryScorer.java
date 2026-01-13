package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.scores.ConstantScore;
import com.xgen.mongot.index.query.scores.DismaxScore;
import com.xgen.mongot.index.query.scores.EmbeddedScore;
import com.xgen.mongot.index.query.scores.FunctionScore;
import com.xgen.mongot.index.query.scores.PathBoostScore;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.index.query.scores.ValueBoostScore;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;

/** A helper class to ensure Lucene queries are consistently scored. */
class LuceneQueryScorer {
  private final LuceneFunctionScoreQueryFactory luceneFunctionScoreQueryFactory;

  private LuceneQueryScorer(LuceneFunctionScoreQueryFactory luceneFunctionScoreQueryFactory) {
    this.luceneFunctionScoreQueryFactory = luceneFunctionScoreQueryFactory;
  }

  public static LuceneQueryScorer create(SearchQueryFactoryContext queryFactoryContext) {
    LuceneFunctionScoreQueryFactory luceneFunctionScoreQueryFactory =
        LuceneFunctionScoreQueryFactory.create(queryFactoryContext);
    return new LuceneQueryScorer(luceneFunctionScoreQueryFactory);
  }

  public Query score(Operator operator, Query unscored, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return switch (operator.getType()) {
      case COMPOUND -> this.scoreCompoundQuery(operator, unscored, singleQueryContext);
      case EMBEDDED_DOCUMENT -> this.scoreEmbeddedQuery(operator, unscored, singleQueryContext);
      case SPAN_CONTAINS, SPAN_FIRST, SPAN_NEAR, SPAN_OR, SPAN_SUBTRACT, SPAN_TERM ->
          this.scoreSpanQuery(operator, (SpanQuery) unscored);
      default -> this.scoreQuery(operator.score(), unscored, singleQueryContext);
    };
  }

  private Query scoreCompoundQuery(
      Operator operator, Query unscored, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    if (operator.score().getType() == Score.Type.DISMAX) {
      return unscored;
    }

    return this.scoreQuery(operator.score(), unscored, singleQueryContext);
  }

  private Query scoreEmbeddedQuery(
      Operator operator, Query unscored, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    if (operator.score() instanceof EmbeddedScore score) {
      // embedded aggregate mode applied in embedded document query factory; only thing left to do
      // here is to apply the outerScore from an embedded score specification, if it is specified.
      return this.scoreQuery(score.outerScore(), unscored, singleQueryContext);
    }

    return this.scoreQuery(operator.score(), unscored, singleQueryContext);
  }

  /**
   * The only types of score currently supported on Span is BOOST.
   *
   * <p>TODO(CLOUDP-280897): Support Constant and Dismax Scores.
   *
   * @param unscored Query
   * @return Query
   */
  private Query scoreSpanQuery(Operator operator, SpanQuery unscored) throws InvalidQueryException {
    Score score = operator.score();
    return switch (score) {
      case ValueBoostScore v when v.isDefault() -> unscored;
      case ValueBoostScore v -> new BoostQuery(unscored, v.boost());
      case PathBoostScore pathBoostScore ->
          throw new InvalidQueryException("Only 'boost' score is allowed for a span query");
      case ConstantScore constantScore ->
          throw new InvalidQueryException("Only 'boost' score is allowed for a span query");
      case DismaxScore dismaxScore ->
          throw new InvalidQueryException("Only 'boost' score is allowed for a span query");
      case FunctionScore functionScore ->
          throw new InvalidQueryException("Only 'boost' score is allowed for a span query");
      case EmbeddedScore embeddedScore ->
          throw new InvalidQueryException("Only 'boost' score is allowed for a span query");
    };
  }

  private Query scoreQuery(Score score, Query query, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return switch (score) {
      case ConstantScore constantScore -> LuceneQueryScorer.fromConstantScore(query, constantScore);
      case ValueBoostScore valueBoostScore ->
          LuceneQueryScorer.fromValueBoost(query, valueBoostScore);
      case PathBoostScore pathBoostScore ->
          this.luceneFunctionScoreQueryFactory.fromPathBoost(
              query, pathBoostScore, singleQueryContext);
      case FunctionScore functionScore ->
          this.luceneFunctionScoreQueryFactory.fromFunction(
              query, functionScore, singleQueryContext);
      case DismaxScore dismaxScore ->
          throw new InvalidQueryException("'dismax' score is not allowed for this query type");
      case EmbeddedScore embeddedScore ->
          throw new InvalidQueryException("'embedded' score is not allowed for this query type");
    };
  }

  private static Query fromConstantScore(Query query, ConstantScore constantScore) {
    // Check type of query here to avoid nesting ConstantScoreQuery.
    ConstantScoreQuery constantScoreQuery =
        query instanceof ConstantScoreQuery
            ? (ConstantScoreQuery) query
            : new ConstantScoreQuery(query);

    if (Float.compare(constantScore.score(), 1f) == 0) {
      return constantScoreQuery;
    }
    return new BoostQuery(constantScoreQuery, constantScore.score());
  }

  private static Query fromValueBoost(Query query, ValueBoostScore boost) {
    if (boost.isDefault()) {
      return query;
    }
    return new BoostQuery(query, boost.boost());
  }
}
