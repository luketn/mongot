package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.query.operators.SpanContainsOperator.SpanToReturn.OUTER;

import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.SpanContainsOperator;
import com.xgen.mongot.index.query.operators.SpanFirstOperator;
import com.xgen.mongot.index.query.operators.SpanNearOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.SpanOrOperator;
import com.xgen.mongot.index.query.operators.SpanSubtractOperator;
import com.xgen.mongot.index.query.operators.SpanTermOperator;
import com.xgen.mongot.util.CheckedStream;
import java.util.List;
import org.apache.lucene.queries.spans.SpanContainingQuery;
import org.apache.lucene.queries.spans.SpanFirstQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanNotQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queries.spans.SpanWithinQuery;

class SpanQueryFactory {

  private final TermQueryFactory termQueryFactory;

  SpanQueryFactory(TermQueryFactory termQueryFactory) {
    this.termQueryFactory = termQueryFactory;
  }

  SpanQuery fromSpan(SpanOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return switch (operator) {
      case SpanContainsOperator spanContainsOperator ->
          fromSpanContains(spanContainsOperator, singleQueryContext);
      case SpanFirstOperator spanFirstOperator ->
          fromSpanFirst(spanFirstOperator, singleQueryContext);
      case SpanNearOperator spanNearOperator -> fromSpanNear(spanNearOperator, singleQueryContext);
      case SpanOrOperator spanOrOperator -> fromSpanOr(spanOrOperator, singleQueryContext);
      case SpanSubtractOperator spanSubtractOperator ->
          fromSpanSubtract(spanSubtractOperator, singleQueryContext);
      case SpanTermOperator spanTermOperator -> fromSpanTerm(spanTermOperator, singleQueryContext);
    };
  }

  private SpanQuery fromSpanContains(
      SpanContainsOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    SpanQuery bigQuery = this.fromSpan(operator.big(), singleQueryContext);
    SpanQuery littleQuery = this.fromSpan(operator.little(), singleQueryContext);

    return operator.spanToReturn() == OUTER
        ? new SpanContainingQuery(bigQuery, littleQuery)
        : new SpanWithinQuery(bigQuery, littleQuery);
  }

  private SpanQuery fromSpanFirst(SpanFirstOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    SpanQuery operatorQuery = this.fromSpan(operator.operator(), singleQueryContext);
    return new SpanFirstQuery(operatorQuery, operator.endingPosition());
  }

  private SpanQuery fromSpanNear(SpanNearOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    SpanQuery[] queries = buildSpanQueries(operator.clauses(), singleQueryContext);
    return new SpanNearQuery(queries, operator.slop(), operator.inOrder());
  }

  private SpanQuery fromSpanOr(SpanOrOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    SpanQuery[] queries = buildSpanQueries(operator.clauses(), singleQueryContext);
    return new SpanOrQuery(queries);
  }

  private SpanQuery fromSpanSubtract(
      SpanSubtractOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    SpanQuery include = this.fromSpan(operator.include(), singleQueryContext);
    SpanQuery exclude = this.fromSpan(operator.exclude(), singleQueryContext);
    return new SpanNotQuery(include, exclude);
  }

  private SpanQuery fromSpanTerm(SpanTermOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    if (operator.termOperator().getType() != Operator.Type.TERM) {
      return fromSpanMultiTerm(operator, singleQueryContext);
    }

    SpanQuery[] queries =
        CheckedStream.from(
                StringPathQuery.product(
                    operator.termOperator().paths(), operator.termOperator().query()))
            .mapAndCollectChecked(
                pq -> this.termQueryFactory.normalizedTerm(pq, singleQueryContext))
            .stream()
            .map(SpanTermQuery::new)
            .toArray(SpanQuery[]::new);

    // avoid wrapping a single query in a spanOr
    if (queries.length == 1) {
      return queries[0];
    }

    return new SpanOrQuery(queries);
  }

  private SpanQuery fromSpanMultiTerm(
      SpanTermOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    var termQueryBuilder =
        this.termQueryFactory.getMultiTermQueryBuilder(operator.termOperator(), singleQueryContext);

    SpanQuery[] queries =
        CheckedStream.from(
                StringPathQuery.product(
                    operator.termOperator().paths(), operator.termOperator().query()))
            .mapAndCollectChecked(termQueryBuilder)
            .stream()
            .map(SpanMultiTermQueryWrapper::new)
            .toArray(SpanMultiTermQueryWrapper[]::new);

    return new SpanOrQuery(queries);
  }

  private SpanQuery[] buildSpanQueries(
      List<SpanOperator> clauses, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    List<SpanQuery> spanQueries =
        CheckedStream.from(clauses)
            .mapAndCollectChecked(operator -> fromSpan(operator, singleQueryContext));
    return spanQueries.toArray(SpanQuery[]::new);
  }
}
