package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Comparator;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record DocAndScoreQuerySpec() implements LuceneQuerySpecification {

  public static DocAndScoreQuerySpec fromBson(DocumentParser parser) {
    return new DocAndScoreQuerySpec();
  }

  @Override
  public Type getType() {
    return Type.DOC_AND_SCORE_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return other.getType() == this.getType();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().build();
  }
}
