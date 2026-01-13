package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.TermQuerySpec;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import org.apache.lucene.index.Term;
import org.junit.Assert;
import org.junit.Test;

public class TermQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.TermQuery query =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

    TermQuerySpec expected = new TermQuerySpec(FieldPath.parse("foo"), "bar");
    TermQuerySpec result = TermQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryNestedField() {
    org.apache.lucene.search.TermQuery query =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo.bar.baz", "bar"));

    TermQuerySpec expected = new TermQuerySpec(FieldPath.parse("foo.bar.baz"), "bar");
    TermQuerySpec result = TermQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryMultiField() {
    org.apache.lucene.search.TermQuery query =
        new org.apache.lucene.search.TermQuery(new Term("$multi/foo.bar.baz.buzz", "buzz!"));

    TermQuerySpec expected = new TermQuerySpec(FieldPath.parse("foo.bar.baz.buzz"), "buzz!");
    TermQuerySpec result = TermQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
