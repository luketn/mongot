package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Comparator;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record MatchNoDocsQuerySpec() implements LuceneQuerySpecification {
  @Override
  public Type getType() {
    return Type.MATCH_NO_DOCS_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    return this.getType() == other.getType();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().build();
  }

  public static MatchNoDocsQuerySpec fromBson(DocumentParser parser) {
    return new MatchNoDocsQuerySpec();
  }
}
