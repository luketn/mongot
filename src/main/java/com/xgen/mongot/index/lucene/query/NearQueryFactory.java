package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.GeoPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.util.FieldPath;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

/** Create a near query for NearOperator. Process queries with a factory specific to their type. */
class NearQueryFactory {

  private final SearchQueryFactoryContext queryFactoryContext;

  NearQueryFactory(SearchQueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  Query fromOperator(NearOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    BooleanComposer.StreamUtils<FieldPath> paths =
        BooleanComposer.StreamUtils.from(operator.paths());

    return switch (operator.origin()) {
      case DatePoint datePoint ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              DateNearQueryFactory.dateQuery(datePoint, operator.pivot()),
              BooleanClause.Occur.SHOULD);
      case GeoPoint geoPoint ->
          paths.mapCheckedWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              GeoNearQueryFactory.geoQuery(geoPoint, operator.pivot(), this.queryFactoryContext),
              BooleanClause.Occur.SHOULD);
      case NumericPoint numericPoint ->
          paths.mapWithBoundSecondArgument(
              singleQueryContext.getEmbeddedRoot(),
              NumericNearQueryFactory.numericQuery(numericPoint, operator.pivot()),
              BooleanClause.Occur.SHOULD);
      default -> throw new AssertionError("near operator has undefined subtype");
    };
  }
}
