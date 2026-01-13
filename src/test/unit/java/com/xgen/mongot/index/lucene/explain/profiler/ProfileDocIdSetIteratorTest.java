package com.xgen.mongot.index.lucene.explain.profiler;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.Test;

public class ProfileDocIdSetIteratorTest {
  @Test
  public void testIterator() throws Exception {
    ExplainTimings timings = spy(ExplainTimings.builder().build());
    ProfileDocIdSetIterator iterator =
        ProfileDocIdSetIterator.create(DocIdSetIterator.all(10), timings);

    iterator.advance(3);
    verify(timings, times(1)).split(ExplainTimings.Type.ADVANCE);

    iterator.nextDoc();
    iterator.nextDoc();
    verify(timings, times(2)).split(ExplainTimings.Type.NEXT_DOC);

    Truth.assertThat(iterator.docID()).isEqualTo(5);

    Truth.assertThat(iterator.advance(11)).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
    verify(timings, times(2)).split(ExplainTimings.Type.ADVANCE);
  }
}
