package com.xgen.mongot.index.lucene.query.range;

import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.Point;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.commons.lang3.Range;

/**
 * An {@code InclusiveRangeFactory} converts user-supplied RangeBoundDefinitions to {@linkplain
 * Range}s. Ranges contain inclusive bounds that are ready to use in a Lucene query.
 *
 * <p>This class is responsible for:
 *
 * <p>(1) Checking if range bounds overlap with numeric representation. For example, if {@code <T>}
 * is of type {@code Long}, InclusiveRangeFactory will determine that range [1.0E42, 2.0E42] does
 * not intersect the representable range for {@code Long} values.
 *
 * <p>(2) Transforming user-supplied values to clamped, inclusive ones. Lucene ranges are specified
 * by inclusive bounds; the InclusiveRangeFactory takes what might be provided by a user as an
 * exclusive bound and transforms it to whatever inclusive boundary is adjacent. For long values,
 * this process might take range (3, any] and transform it to [4, any].
 *
 * @param <P> user-provided bounds type.
 * @param <T> output type.
 */
public class InclusiveRangeFactory<P extends Point & Comparable<P>, T extends Comparable<T>> {

  private final P minPoint;
  private final P maxPoint;
  private final Function<RangeBound<P>, T> lowerBoundSupplier;
  private final Function<RangeBound<P>, T> upperBoundSupplier;

  public InclusiveRangeFactory(
      P minPoint,
      P maxPoint,
      Function<RangeBound<P>, T> lowerBoundSupplier,
      Function<RangeBound<P>, T> upperBoundSupplier) {
    this.minPoint = minPoint;
    this.maxPoint = maxPoint;
    this.lowerBoundSupplier = lowerBoundSupplier;
    this.upperBoundSupplier = upperBoundSupplier;
  }

  /**
   * Returns an inclusive range for the provided bounds, or an empty optional if the query is over a
   * set outside the representable range of type {@code <T>}.
   *
   * <p>This function performs these steps:
   *
   * <ol>
   *   <li>Verify that given bounds are representable over values of type {@code <T>}. If it is not,
   *       return an empty Optional.
   *   <li>Transform the given bounds to inclusive ones.
   *   <li>Return that value.
   * </ol>
   *
   * <p>Examples of bounds that are not representable for values of type {@code Long}:
   *
   * <ul>
   *   <li>[0.1, 0.9]
   *   <li>[1.0E42, 2.0E42]
   * </ul>
   *
   * @param bound user-provided bounds.
   * @return inclusive bounds.
   */
  public Optional<Range<T>> createRange(RangeBound<P> bound) {
    if (isOutOfBounds(bound)) {
      return Optional.empty();
    }

    T lowerBound = this.lowerBoundSupplier.apply(bound);
    T upperBound = this.upperBoundSupplier.apply(bound);
    if (lowerBound.compareTo(upperBound) > 0) {
      return Optional.empty();
    }

    return Optional.of(Range.of(lowerBound, upperBound));
  }

  private boolean isOutOfBounds(RangeBound<P> rangeBounds) {
    // [any, MIN - 1]
    Predicate<RangeBound<P>> inclusiveUpperLtLowestPossible =
        (RangeBound<P> bounds) ->
            bounds
                .getUpper()
                .map(upper -> upper.compareTo(this.minPoint) < 0 && bounds.upperInclusive())
                .orElse(false);

    // [any, MIN)
    Predicate<RangeBound<P>> exclusiveUpperLteLowestPossible =
        (RangeBound<P> bounds) ->
            bounds
                .getUpper()
                .map(upper -> upper.compareTo(this.minPoint) <= 0 && !bounds.upperInclusive())
                .orElse(false);

    // [MAX + 1, any]
    Predicate<RangeBound<P>> inclusiveLowerGtGreatestPossible =
        (RangeBound<P> bounds) ->
            bounds
                .getLower()
                .map(lower -> lower.compareTo(this.maxPoint) > 0 && bounds.lowerInclusive())
                .orElse(false);

    // (MAX, any]
    Predicate<RangeBound<P>> exclusiveLowerGteGreatestPossible =
        (RangeBound<P> bounds) ->
            bounds
                .getLower()
                .map(lower -> lower.compareTo(this.maxPoint) >= 0 && !bounds.lowerInclusive())
                .orElse(false);

    return inclusiveUpperLtLowestPossible
        .or(exclusiveUpperLteLowestPossible)
        .or(inclusiveLowerGtGreatestPossible)
        .or(exclusiveLowerGteGreatestPossible)
        .test(rangeBounds);
  }
}
