package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.range.InclusiveRangeFactory;
import com.xgen.mongot.index.lucene.query.range.RangeBoundToDoubleTransformer;
import com.xgen.mongot.index.lucene.query.range.RangeBoundToLongTransformer;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.commons.lang3.Range;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

public class NumericRangeQueryFactory {

  private static final InclusiveRangeFactory<NumericPoint, Long> LONG_INCLUSIVE_RANGE_FACTORY =
      new InclusiveRangeFactory<>(
          new LongPoint(Long.MIN_VALUE),
          new LongPoint(Long.MAX_VALUE),
          RangeBoundToLongTransformer::getLower,
          RangeBoundToLongTransformer::getUpper);

  private static final InclusiveRangeFactory<NumericPoint, Double> DOUBLE_INCLUSIVE_RANGE_FACTORY =
      new InclusiveRangeFactory<>(
          new DoublePoint(-1.0 * Double.MAX_VALUE),
          new DoublePoint(Double.MAX_VALUE),
          RangeBoundToDoubleTransformer::getLower,
          RangeBoundToDoubleTransformer::getUpper);

  /**
   * Returns a function that produces queries based on a path option definition.
   *
   * @param bound the bounds of the range to be applied in the returned function.
   * @return a query producing function.
   */
  public static CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException>
      fromBounds(RangeBound<NumericPoint> bound) {
    return (path, embeddedRoot) ->
        BooleanComposer.constantScoreDisjunction(
            longQuery(bound).apply(path, embeddedRoot),
            doubleQuery(bound).apply(path, embeddedRoot));
  }

  private static BiFunction<FieldPath, Optional<FieldPath>, Query> longQuery(
      RangeBound<NumericPoint> bounds) {
    Optional<Range<Long>> inclusiveRange = LONG_INCLUSIVE_RANGE_FACTORY.createRange(bounds);

    return (path, embeddedRoot) ->
        inclusiveRange
            .map(
                simpleBounds ->
                    RangeQueryUtils.createLuceneRangeQuery(
                        FieldName.TypeField.NUMBER_INT64.getLuceneFieldName(path, embeddedRoot),
                        FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
                            path, embeddedRoot),
                        simpleBounds.getMinimum(),
                        simpleBounds.getMaximum()))
            .orElseGet(
                () ->
                    new MatchNoDocsQuery(
                        bounds + " bounds are outside representable range of long"));
  }

  private static BiFunction<FieldPath, Optional<FieldPath>, Query> doubleQuery(
      RangeBound<NumericPoint> bounds) {
    Optional<Range<Double>> inclusiveRange = DOUBLE_INCLUSIVE_RANGE_FACTORY.createRange(bounds);

    return (path, embeddedRoot) ->
        inclusiveRange
            .map(
                simpleBounds ->
                    RangeQueryUtils.createLuceneRangeQuery(
                        FieldName.TypeField.NUMBER_DOUBLE.getLuceneFieldName(path, embeddedRoot),
                        FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(
                            path, embeddedRoot),
                        LuceneDoubleConversionUtils.toLong(simpleBounds.getMinimum()),
                        LuceneDoubleConversionUtils.toLong(simpleBounds.getMaximum())))
            .orElseGet(
                () ->
                    new MatchNoDocsQuery(
                        bounds + " bounds are outside representable range of double"));
  }
}
