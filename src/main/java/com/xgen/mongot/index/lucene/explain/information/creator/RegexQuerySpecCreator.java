package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.RegexQuerySpec;
import org.apache.lucene.search.RegexpQuery;

public class RegexQuerySpecCreator {
  static RegexQuerySpec fromQuery(RegexpQuery q) {
    return new RegexQuerySpec(
        LuceneQuerySpecificationCreator.strip(q.getRegexp().field()), q.getRegexp().text());
  }
}
