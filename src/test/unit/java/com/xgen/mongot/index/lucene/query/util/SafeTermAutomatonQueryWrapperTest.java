package com.xgen.mongot.index.lucene.query.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.lucene.sandbox.search.TermAutomatonQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.junit.Assert;
import org.junit.Test;

public class SafeTermAutomatonQueryWrapperTest {

  @Test
  public void testRewriteToNonTermAutomatonQuery() throws IOException {
    TermAutomatonQuery originalQuery = mock(TermAutomatonQuery.class);
    Query rewrittenQuery = mock(Query.class);

    when(originalQuery.rewrite(any(IndexSearcher.class))).thenReturn(rewrittenQuery);

    Assert.assertEquals(
        "should return unwrapped query when rewritten to non-TermAutomatonQuery",
        rewrittenQuery,
        SafeTermAutomatonQueryWrapper.create(originalQuery).rewrite(mock(IndexSearcher.class)));
  }

  @Test
  public void testRewriteToTermAutomatonQuery() throws IOException {
    TermAutomatonQuery originalQuery = mock(TermAutomatonQuery.class);
    TermAutomatonQuery rewrittenQuery = mock(TermAutomatonQuery.class);

    when(originalQuery.rewrite(any(IndexSearcher.class))).thenReturn(rewrittenQuery);
    Query expected = SafeTermAutomatonQueryWrapper.create(rewrittenQuery);

    Assert.assertEquals(
        "should wrap query when TermAutomatonQuery is rewritten to another TermAutomatonQuery",
        expected,
        SafeTermAutomatonQueryWrapper.create(originalQuery).rewrite(mock(IndexSearcher.class)));
  }

  @Test
  public void testRewriteToSameObject() throws IOException {
    TermAutomatonQuery originalQuery = mock(TermAutomatonQuery.class);

    when(originalQuery.rewrite(any(IndexSearcher.class))).thenReturn(originalQuery);

    SafeTermAutomatonQueryWrapper originalQueryWrapper =
        SafeTermAutomatonQueryWrapper.create(originalQuery);

    Assert.assertSame(
        "should return same object when wrapped query is rewritten to itself",
        originalQueryWrapper,
        originalQueryWrapper.rewrite(mock(IndexSearcher.class)));
  }

  @Test
  public void testCreateWeightScoreModes() throws IOException {
    TermAutomatonQuery originalQuery = mock(TermAutomatonQuery.class);
    when(originalQuery.createWeight(any(), any(), anyFloat())).thenReturn(mock(Weight.class));

    SafeTermAutomatonQueryWrapper wrapper = SafeTermAutomatonQueryWrapper.create(originalQuery);

    clearInvocations(originalQuery);
    wrapper.createWeight(mock(IndexSearcher.class), ScoreMode.COMPLETE, 1.0f);
    verify(originalQuery).createWeight(any(), eq(ScoreMode.COMPLETE), anyFloat());

    clearInvocations(originalQuery);
    wrapper.createWeight(mock(IndexSearcher.class), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    verify(originalQuery).createWeight(any(), eq(ScoreMode.COMPLETE), anyFloat());

    clearInvocations(originalQuery);
    wrapper.createWeight(mock(IndexSearcher.class), ScoreMode.TOP_SCORES, 1.0f);
    verify(originalQuery).createWeight(any(), eq(ScoreMode.TOP_SCORES), anyFloat());

    verifyNoMoreInteractions(originalQuery);
  }
}
