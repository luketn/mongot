package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.WildcardQuerySpec;
import org.apache.lucene.search.WildcardQuery;

public class WildcardQuerySpecCreator {
  static WildcardQuerySpec fromQuery(WildcardQuery q) {
    return new WildcardQuerySpec(
        LuceneQuerySpecificationCreator.strip(q.getField()), q.getTerm().text());
  }
}
