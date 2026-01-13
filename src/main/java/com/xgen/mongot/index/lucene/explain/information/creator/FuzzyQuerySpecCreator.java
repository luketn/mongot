package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.FuzzyQuerySpec;
import org.apache.lucene.search.FuzzyQuery;

public class FuzzyQuerySpecCreator {
  static FuzzyQuerySpec fromQuery(FuzzyQuery q) {
    return new FuzzyQuerySpec(
        LuceneQuerySpecificationCreator.strip(q.getField()), q.getTerm().text());
  }
}
