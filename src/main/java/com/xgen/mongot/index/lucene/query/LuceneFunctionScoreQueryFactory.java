package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.values.AddValuesSource;
import com.xgen.mongot.index.lucene.query.values.GaussianDecayValuesSource;
import com.xgen.mongot.index.lucene.query.values.LogValuesSource;
import com.xgen.mongot.index.lucene.query.values.MultiplyValuesSource;
import com.xgen.mongot.index.lucene.query.values.PathValuesSource;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.scores.FunctionScore;
import com.xgen.mongot.index.query.scores.PathBoostScore;
import com.xgen.mongot.index.query.scores.expressions.AddExpression;
import com.xgen.mongot.index.query.scores.expressions.ConstantExpression;
import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.index.query.scores.expressions.GaussianDecayExpression;
import com.xgen.mongot.index.query.scores.expressions.Log1PExpression;
import com.xgen.mongot.index.query.scores.expressions.LogExpression;
import com.xgen.mongot.index.query.scores.expressions.MultiplyExpression;
import com.xgen.mongot.index.query.scores.expressions.PathExpression;
import com.xgen.mongot.index.query.scores.expressions.ScoreExpression;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;

class LuceneFunctionScoreQueryFactory {

  private final SearchQueryFactoryContext queryFactoryContext;

  private LuceneFunctionScoreQueryFactory(SearchQueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  static LuceneFunctionScoreQueryFactory create(SearchQueryFactoryContext queryFactoryContext) {
    return new LuceneFunctionScoreQueryFactory(queryFactoryContext);
  }

  FunctionScoreQuery fromPathBoost(
      Query query, PathBoostScore pathBoostScore, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    PathExpression pathExpression =
        new PathExpression(pathBoostScore.path(), pathBoostScore.undefined());
    PathValuesSource fieldValue = buildPathValuesSource(pathExpression, singleQueryContext);
    return FunctionScoreQuery.boostByValue(query, fieldValue);
  }

  FunctionScoreQuery fromFunction(
      Query query, FunctionScore functionScore, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    // Get the top-level expression of the function score
    Expression expression = functionScore.expression();

    DoubleValuesSource expressionTreeValuesSource =
        buildExpressionTreeValuesSource(expression, singleQueryContext);

    return new FunctionScoreQuery(query, expressionTreeValuesSource);
  }

  private DoubleValuesSource buildExpressionTreeValuesSource(
      Expression expression, SingleQueryContext singleQueryContext) throws InvalidQueryException {

    return switch (expression) {
      case AddExpression addExpression -> {
        List<DoubleValuesSource> addArgs =
            CheckedStream.from(addExpression.arguments())
                .mapAndCollectChecked(
                    expression1 ->
                        buildExpressionTreeValuesSource(expression1, singleQueryContext));

        yield AddValuesSource.create(addArgs);
      }
      case ConstantExpression constantExpression ->
          DoubleValuesSource.constant(constantExpression.constant());
      case GaussianDecayExpression gaussianDecayExpression -> {
        DoubleValuesSource pathValue =
            buildExpressionTreeValuesSource(gaussianDecayExpression.path(), singleQueryContext);
        double originValue = gaussianDecayExpression.origin();
        double scaleValue = gaussianDecayExpression.scale();
        double offsetValue = gaussianDecayExpression.offset();
        double decayValue = gaussianDecayExpression.decay();
        yield GaussianDecayValuesSource.create(
            pathValue, originValue, scaleValue, offsetValue, decayValue);
      }
      case MultiplyExpression multiplyExpression -> {
        List<DoubleValuesSource> multiplyArgs =
            CheckedStream.from(multiplyExpression.arguments())
                .mapAndCollectChecked(
                    expression1 ->
                        buildExpressionTreeValuesSource(expression1, singleQueryContext));

        yield MultiplyValuesSource.create(multiplyArgs);
      }
      case LogExpression logExpression -> {
        DoubleValuesSource logArgValue =
            buildExpressionTreeValuesSource(logExpression.argument(), singleQueryContext);
        yield LogValuesSource.create(logArgValue, false);
      }
      case Log1PExpression log1PExpression -> {
        DoubleValuesSource log1PArgValue =
            buildExpressionTreeValuesSource(log1PExpression.argument(), singleQueryContext);
        yield LogValuesSource.create(log1PArgValue, true);
      }
      case PathExpression pathExpression ->
          buildPathValuesSource(pathExpression, singleQueryContext);
      case ScoreExpression scoreExpression -> DoubleValuesSource.SCORES;
    };
  }

  private PathValuesSource buildPathValuesSource(
      PathExpression pathExpression, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    FieldPath path = pathExpression.path();
    double undefinedValue = pathExpression.undefined();

    NumericFieldOptions.Representation representation =
        this.queryFactoryContext
            .getNumericRepresentation(path, singleQueryContext.getEmbeddedRoot())
            .orElseThrow(
                () ->
                    new InvalidQueryException(
                        String.format(
                            "path expression for function score requires path \"%s\" to be indexed"
                                + " as numeric",
                            path)));

    return switch (representation) {
      case INT64 -> {
        String longLuceneFieldName =
            FieldName.TypeField.NUMBER_INT64.getLuceneFieldName(
                path, singleQueryContext.getEmbeddedRoot());

        DoubleValuesSource doubleValuesSourceFromLongField =
            DoubleValuesSource.fromLongField(longLuceneFieldName);

        yield PathValuesSource.create(doubleValuesSourceFromLongField, undefinedValue, path);
      }
      case DOUBLE -> {
        String doubleLuceneFieldName =
            FieldName.TypeField.NUMBER_DOUBLE.getLuceneFieldName(
                path, singleQueryContext.getEmbeddedRoot());

        DoubleValuesSource doubleValuesSourceFromDoubleField =
            DoubleValuesSource.fromField(
                doubleLuceneFieldName, LuceneDoubleConversionUtils::fromLong);

        yield PathValuesSource.create(doubleValuesSourceFromDoubleField, undefinedValue, path);
      }
    };
  }
}
