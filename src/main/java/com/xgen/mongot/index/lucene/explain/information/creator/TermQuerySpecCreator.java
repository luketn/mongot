package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.TermQuerySpec;
import org.apache.lucene.search.TermQuery;

public class TermQuerySpecCreator {
  public static TermQuerySpec fromQuery(TermQuery query) {
    return new TermQuerySpec(query.getTerm());
  }
}
