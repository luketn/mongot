package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.DefaultQuerySpec;
import org.apache.lucene.search.Query;

public class DefaultQuerySpecCreator {
  static DefaultQuerySpec fromQuery(Query query) {
    return new DefaultQuerySpec(query.getClass().getSimpleName());
  }
}
