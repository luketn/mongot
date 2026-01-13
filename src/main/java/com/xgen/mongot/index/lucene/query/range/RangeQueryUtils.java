package com.xgen.mongot.index.lucene.query.range;

import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;

public final class RangeQueryUtils {

  public static Query createLuceneRangeQuery(
      String singleValueField, String multiValueField, long lowerBound, long upperBound) {

    // Lucene selects LongPoint or DocValues approach based on runtime statistics
    var singleValueQuery =
        new IndexOrDocValuesQuery(
            createLongPointQuery(singleValueField, lowerBound, upperBound),
            createDocValuesQuery(singleValueField, lowerBound, upperBound));

    // we do not apply optimization for array fields because they are not indexed as DocValues
    var multiValueQuery = createLongPointQuery(multiValueField, lowerBound, upperBound);

    return BooleanComposer.constantScoreDisjunction(singleValueQuery, multiValueQuery);
  }

  public static Query createLongPointQuery(String fieldName, long lowerBound, long upperBound) {
    return org.apache.lucene.document.LongPoint.newRangeQuery(fieldName, lowerBound, upperBound);
  }

  public static Query createDocValuesQuery(String fieldName, long lowerBound, long upperBound) {
    return NumericDocValuesField.newSlowRangeQuery(fieldName, lowerBound, upperBound);
  }
}
