package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.PointInSetQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.document.LongPoint;

public class PointInSetQuerySpecCreator {
  static <T extends Optional<? extends QueryChildren>> PointInSetQuerySpec fromQuery(
      org.apache.lucene.search.PointInSetQuery query) {
    var points =
        query.getPackedPoints().stream()
            .map(p -> LongPoint.decodeDimension(p, 0))
            .collect(Collectors.toList());
    return new PointInSetQuerySpec(
        FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(query.getField())), points);
  }
}
