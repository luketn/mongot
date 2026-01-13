package com.xgen.mongot.index.lucene.query;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.BooleanRangeBound;
import com.xgen.mongot.index.query.operators.value.BooleanValue;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

public class BooleanRangeQueryFactory {

  private final EqualsQueryFactory equalsQueryFactory;

  public BooleanRangeQueryFactory(EqualsQueryFactory equalsQueryFactory) {
    this.equalsQueryFactory = equalsQueryFactory;
  }

  @VisibleForTesting
  enum EffectiveValue {
    TRUE,
    FALSE,
    EITHER,
    NEITHER;

    public EffectiveValue and(EffectiveValue other) {

      if (this == NEITHER || other == NEITHER) {
        return NEITHER;
      }

      if (other == EITHER || other == this) {
        return this;
      }

      if (this == EITHER) {
        return other;
      }

      return NEITHER;
    }
  }

  /** Converts range query to equals query according to MQL-like semantics. */
  public CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException> fromBounds(
      BooleanRangeBound bound) {
    return (path, embeddedRoot) -> {
      EffectiveValue lowerBoundValue =
          bound
              .getLower()
              .map(point -> lowerBound(point.value(), bound.lowerInclusive()))
              .orElse(EffectiveValue.EITHER);

      EffectiveValue upperBoundValue =
          bound
              .getUpper()
              .map(point -> upperBound(point.value(), bound.upperInclusive()))
              .orElse(EffectiveValue.EITHER);

      EffectiveValue conjunction = lowerBoundValue.and(upperBoundValue);

      return convertToQuery(conjunction, path, embeddedRoot);
    };
  }

  /**
   * Returns the effective value for lower bound.
   *
   * <pre>
   * {$gt: false} matches true
   * {$gte: false} matches true and false
   * {$gt: true} matches nothing
   * {$gte: true} matches true
   * </pre>
   */
  private EffectiveValue lowerBound(boolean value, boolean inclusive) {

    if (inclusive) {
      return value ? EffectiveValue.TRUE : EffectiveValue.EITHER;
    }

    return value ? EffectiveValue.NEITHER : EffectiveValue.TRUE;
  }

  /**
   * Returns the effective value for upper bound.
   *
   * <pre>
   * {$lt: false} matches nothing
   * {$lte: false} matches false
   * {$lt: true} matches false
   * {$lte: true} matches true and false
   * </pre>
   */
  private EffectiveValue upperBound(boolean value, boolean inclusive) {

    if (inclusive) {
      return value ? EffectiveValue.EITHER : EffectiveValue.FALSE;
    }

    return value ? EffectiveValue.FALSE : EffectiveValue.NEITHER;
  }

  private Query convertToQuery(
      EffectiveValue effectiveValue, FieldPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    return switch (effectiveValue) {
      case TRUE -> this.equalsQueryFactory.fromValue(new BooleanValue(true), path, embeddedRoot);
      case FALSE -> this.equalsQueryFactory.fromValue(new BooleanValue(false), path, embeddedRoot);
      case EITHER ->
          new BooleanQuery.Builder()
              .add(
                  this.equalsQueryFactory.fromValue(new BooleanValue(true), path, embeddedRoot),
                  BooleanClause.Occur.SHOULD)
              .add(
                  this.equalsQueryFactory.fromValue(new BooleanValue(false), path, embeddedRoot),
                  BooleanClause.Occur.SHOULD)
              .build();
      case NEITHER -> new MatchNoDocsQuery();
    };
  }
}
