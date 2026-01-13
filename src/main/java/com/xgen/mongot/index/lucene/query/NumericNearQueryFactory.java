package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.filterClause;
import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.queryFor;
import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.shouldClause;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.util.Rescoring;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.DoublePredicate;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

class NumericNearQueryFactory {

  private static class DoubleValidation {
    private final DoublePredicate predicate;
    private final String errorMessage;

    DoubleValidation(DoublePredicate predicate, String errorMessage) {
      this.predicate = predicate;
      this.errorMessage = errorMessage;
    }

    boolean test(double value) {
      return this.predicate.test(value);
    }

    String getErrorMessage() {
      return this.errorMessage;
    }
  }

  private static final DoubleValidation MAGNITUDE_LT_ONE =
      new DoubleValidation(
          value -> value < 1.0, "values of magnitude less than one cannot be represented");

  private static final DoubleValidation MAGNITUDE_GT_MAX_LONG =
      new DoubleValidation(
          value -> Math.abs(value) > Long.MAX_VALUE,
          "values of magnitude greater than MAX_LONG cannot be represented");

  // Score is equal to (pivot)/(pivot + abs(origin - x)) where "x" is a document value at path.
  private static final Expression SCORING_EXPR =
      Crash.because("failed to compile near scoring expression")
          .ifThrows(() -> JavascriptCompiler.compile("pivot / (pivot + abs(origin - value))"));

  static CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException>
      numericQuery(NumericPoint point, double pivot) {
    return (path, embeddedRoot) ->
        queryFor(
            shouldClause(longQuery(point, pivot).apply(path, embeddedRoot)),
            shouldClause(doubleQuery(point, pivot).apply(path, embeddedRoot)));
  }

  private static BiFunction<FieldPath, Optional<FieldPath>, Query> longQuery(
      NumericPoint origin, double pivot) {

    for (DoubleValidation predicate : List.of(MAGNITUDE_LT_ONE, MAGNITUDE_GT_MAX_LONG)) {
      if (predicate.test(pivot)) {
        return (ignored1, ignored2) ->
            new MatchNoDocsQuery(
                String.format(
                    "pivot outside representable range for int64-indexed values: %s",
                    predicate.getErrorMessage()));
      }
    }

    if (MAGNITUDE_GT_MAX_LONG.test(toDouble(origin))) {
      return (ignored1, ignored2) ->
          new MatchNoDocsQuery(
              String.format(
                  "origin outside representable range for int64-indexed values: %s",
                  MAGNITUDE_GT_MAX_LONG.getErrorMessage()));
    }

    return (path, embeddedRoot) ->
        createQuery(
            FieldName.TypeField.NUMBER_INT64.getLuceneFieldName(path, embeddedRoot),
            toLong(origin),
            (long) pivot);
  }

  private static BiFunction<FieldPath, Optional<FieldPath>, Query> doubleQuery(
      NumericPoint point, double pivot) {

    return (path, embeddedRoot) ->
        boundedDoubleQuery(
            FieldName.TypeField.NUMBER_DOUBLE.getLuceneFieldName(path, embeddedRoot),
            toDouble(point),
            pivot);
  }

  private static Query boundedDoubleQuery(String luceneFieldName, double origin, double pivot) {
    // a boolean "should" clause with a correctly scored near query
    BooleanClause scoredQueryClause =
        shouldClause(scoredDoubleQuery(luceneFieldName, origin, pivot));

    // long bits after translating origin to be in indexed-form
    long originBits = LuceneDoubleConversionUtils.toLong(origin);

    // a boolean "filter" clause for values less than or equal to origin
    BooleanClause lowerFilterClause =
        filterClause(LongPoint.newRangeQuery(luceneFieldName, Long.MIN_VALUE, originBits));

    // make sure we don't overflow in computing (origin + 1)
    long originPlusOne = Math.min(originBits, Long.MAX_VALUE - 1L) + 1L;

    // a boolean "filter" clause for values greater than origin
    BooleanClause upperFilterClause =
        filterClause(LongPoint.newRangeQuery(luceneFieldName, originPlusOne, Long.MAX_VALUE));

    // compose scoredQueryClause with both upper and lower filter clauses.
    BooleanClause lowerHalf = shouldClause(queryFor(lowerFilterClause, scoredQueryClause));
    BooleanClause upperHalf = shouldClause(queryFor(upperFilterClause, scoredQueryClause));

    return queryFor(lowerHalf, upperHalf);
  }

  /**
   * Creates a near Query for $type:double indexed data. Applies a scoring function to correct score
   * values computed by Lucene.
   */
  private static Query scoredDoubleQuery(String luceneFieldPath, double origin, double pivot) {
    Query originalQuery =
        createQuery(
            luceneFieldPath,
            LuceneDoubleConversionUtils.toLong(origin),
            LuceneDoubleConversionUtils.toLong(pivot));

    Map<String, DoubleValuesSource> variables =
        Map.ofEntries(
            Map.entry(
                "value",
                DoubleValuesSource.fromField(
                    luceneFieldPath, LuceneDoubleConversionUtils::fromLong)),
            Map.entry("pivot", DoubleValuesSource.constant(pivot)),
            Map.entry("origin", DoubleValuesSource.constant(origin)));

    return Rescoring.rewriteScore(originalQuery, SCORING_EXPR, variables);
  }

  private static long toLong(NumericPoint point) {
    return switch (point) {
      case DoublePoint dp -> (long) dp.value();
      case com.xgen.mongot.index.query.points.LongPoint lp -> lp.value();
    };
  }

  private static double toDouble(NumericPoint point) {
    return switch (point) {
      case DoublePoint dp -> dp.value();
      case com.xgen.mongot.index.query.points.LongPoint lp -> (double) lp.value();
    };
  }

  private static Query createQuery(String luceneFieldPath, long origin, long pivot) {
    return LongField.newDistanceFeatureQuery(luceneFieldPath, 1.0f, origin, pivot);
  }
}
