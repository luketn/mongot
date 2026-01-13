package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Optional;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

public class UuidRangeQueryFactory {

  /**
   * Returns a function that produces queries based on a path option definition.
   *
   * @param bound the bounds of the range to be applied in the returned function.
   * @return a query producing function.
   */
  public static CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException>
      fromBounds(RangeBound<UuidPoint> bound) {
    return (path, embeddedRoot) -> {
      var field = FieldName.TypeField.UUID.getLuceneFieldName(path, embeddedRoot);

      var lower = bound.getLower().map(b -> new BytesRef(b.value().toString()));
      var upper = bound.getUpper().map(b -> new BytesRef(b.value().toString()));

      var indexQuery =
          new TermRangeQuery(
              field,
              lower.orElse(null),
              upper.orElse(null),
              bound.lowerInclusive(),
              bound.upperInclusive());

      var docValuesQuery =
          SortedSetDocValuesField.newSlowRangeQuery(
              field,
              lower.orElse(null),
              upper.orElse(null),
              bound.lowerInclusive(),
              bound.upperInclusive());

      return new IndexOrDocValuesQuery(indexQuery, docValuesQuery);
    };
  }
}
