package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.DefaultQuerySpec;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import org.apache.lucene.index.Term;
import org.junit.Assert;
import org.junit.Test;

public class DefaultQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.Query query =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

    DefaultQuerySpec expected = new DefaultQuerySpec("TermQuery");
    DefaultQuerySpec result = DefaultQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
