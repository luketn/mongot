package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.SynonymQuerySpec;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.SynonymQuerySpecBuilder;
import java.util.List;
import org.apache.lucene.index.Term;
import org.junit.Assert;
import org.junit.Test;

public class SynonymQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.SynonymQuery query =
        new org.apache.lucene.search.SynonymQuery.Builder("$type:string/foo")
            .addTerm(new Term("$type:string/foo", "bar"))
            .build();

    SynonymQuerySpec expected =
        SynonymQuerySpecBuilder.builder().path("foo").values(List.of("bar")).build();
    SynonymQuerySpec result = SynonymQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryNoTerms() {
    org.apache.lucene.search.SynonymQuery query =
        new org.apache.lucene.search.SynonymQuery.Builder("$type:string/foo").build();

    SynonymQuerySpec expected = SynonymQuerySpecBuilder.builder().build();
    SynonymQuerySpec result = SynonymQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
