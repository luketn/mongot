package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.range.InclusiveRangeFactory;
import com.xgen.mongot.index.lucene.query.range.RangeBoundToDateTransformer;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Date;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

public class DateRangeQueryFactory {

  private static final InclusiveRangeFactory<DatePoint, Date> DATE_RANGE_SIMPLIFIER =
      new InclusiveRangeFactory<>(
          new DatePoint(new Date(Long.MIN_VALUE)),
          new DatePoint(new Date(Long.MAX_VALUE)),
          RangeBoundToDateTransformer::getLower,
          RangeBoundToDateTransformer::getUpper);

  /**
   * Returns a function that produces queries based on a path option definition.
   *
   * @param bound the bounds of the range to be applied in the returned function.
   * @return a query producing function.
   */
  public static CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException>
      fromBounds(RangeBound<DatePoint> bound) {
    Optional<Range<Date>> inclusiveRange = DATE_RANGE_SIMPLIFIER.createRange(bound);

    return (path, embeddedRoot) ->
        inclusiveRange
            .map(
                (simpleBounds) ->
                    RangeQueryUtils.createLuceneRangeQuery(
                        FieldName.TypeField.DATE.getLuceneFieldName(path, embeddedRoot),
                        FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(path, embeddedRoot),
                        simpleBounds.getMinimum().getTime(),
                        simpleBounds.getMaximum().getTime()))
            .orElseGet(
                () ->
                    new MatchNoDocsQuery(
                        bound + " bounds are outside representable range of date"));
  }
}
