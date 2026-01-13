package com.xgen.mongot.index.lucene.explain.profiler;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

public class ProfileDocIdSetIterator extends DocIdSetIterator {
  private final DocIdSetIterator iterator;
  private final ExplainTimings timings;

  public static ProfileDocIdSetIterator create(DocIdSetIterator iterator, ExplainTimings timings) {
    return new ProfileDocIdSetIterator(iterator, timings);
  }

  private ProfileDocIdSetIterator(DocIdSetIterator iterator, ExplainTimings timings) {
    this.iterator = iterator;
    this.timings = timings;
  }

  @Override
  public int advance(int target) throws IOException {
    try (var ignored = this.timings.split(ExplainTimings.Type.ADVANCE)) {
      return this.iterator.advance(target);
    }
  }

  @Override
  public int nextDoc() throws IOException {
    try (var ignored = this.timings.split(ExplainTimings.Type.NEXT_DOC)) {
      return this.iterator.nextDoc();
    }
  }

  @Override
  public int docID() {
    return this.iterator.docID();
  }

  @Override
  public long cost() {
    return this.iterator.cost();
  }

  public ExplainTimings getExplainTimings() {
    return this.timings;
  }
}
