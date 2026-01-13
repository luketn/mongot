package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Optional;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.Query;

class DateNearQueryFactory {

  static CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException> dateQuery(
      DatePoint origin, double pivot) {

    return (path, embeddedRoot) ->
        LongField.newDistanceFeatureQuery(
            FieldName.TypeField.DATE.getLuceneFieldName(path, embeddedRoot),
            1.0f,
            origin.value().getTime(),
            Double.valueOf(pivot).longValue());
  }
}
