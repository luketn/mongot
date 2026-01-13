package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.PrefixQuerySpec;
import org.apache.lucene.search.PrefixQuery;

public class PrefixQuerySpecCreator {
  static PrefixQuerySpec fromQuery(PrefixQuery q) {
    return new PrefixQuerySpec(
        LuceneQuerySpecificationCreator.strip(q.getPrefix().field()), q.getPrefix().text());
  }
}
