package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Comparator;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record SortedSetDocValuesRangeQuerySpec() implements LuceneQuerySpecification {
  static SortedSetDocValuesRangeQuerySpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new SortedSetDocValuesRangeQuerySpec();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().build();
  }

  @Override
  public LuceneQuerySpecification.Type getType() {
    return LuceneQuerySpecification.Type.SORTED_SET_DOC_VALUES_RANGE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == getType();
  }
}
