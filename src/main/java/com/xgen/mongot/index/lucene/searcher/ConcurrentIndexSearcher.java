package com.xgen.mongot.index.lucene.searcher;

import java.util.concurrent.Executor;

public class ConcurrentIndexSearcher extends LuceneIndexSearcher {

  public ConcurrentIndexSearcher(LuceneIndexSearcher other, Executor executor) {
    super(other, executor);
  }
}
