package com.xgen.mongot.index.lucene.query.util;

import java.util.Optional;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import org.junit.Assert;
import org.junit.Test;

public class SafeOperatorQueryBuilderTest {

  @Test
  public void testBooleanResultIsSameWhenPresent() {
    var fid = "fid";
    var query = "quick brown fox";

    Query expected = new QueryBuilder(new StandardAnalyzer()).createBooleanQuery(fid, query);
    Optional<Query> actual = builder().createBooleanQuery(fid, query);

    Assert.assertTrue(actual.isPresent());
    Assert.assertEquals("query", expected, actual.get());
  }

  @Test
  public void testPhraseResultNotPresentForEmptyText() {
    var query = builder().createPhraseQuery("fid", "", 3);
    Assert.assertTrue(query.isEmpty());
  }

  @Test
  public void testContainsPhraseQuery() {
    var query = builder().createPhraseQuery("fid", "hello world", 3);
    Assert.assertTrue(SafeQueryBuilder.containsPhraseQuery(query));
  }

  @Test
  public void testContainsPhraseQueryNested() {
    var query =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    new MultiPhraseQuery.Builder().add(new Term("a", "hello")).build(),
                    BooleanClause.Occur.SHOULD))
            .build();
    Assert.assertTrue(SafeQueryBuilder.containsPhraseQuery(Optional.of(query)));
  }

  private SafeQueryBuilder builder() {
    return new SafeQueryBuilder(new StandardAnalyzer());
  }
}
