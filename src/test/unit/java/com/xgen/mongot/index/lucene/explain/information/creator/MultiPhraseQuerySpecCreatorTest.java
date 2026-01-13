package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.MultiPhraseQuerySpec;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiPhraseQuery;
import org.junit.Assert;
import org.junit.Test;

public class MultiPhraseQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    String field = "$type:string/foo";
    MultiPhraseQuery query =
        new MultiPhraseQuery.Builder()
            .add(new Term[] {new Term(field, "bar"), new Term(field, "baz")})
            .add(new Term(field, "quux"))
            .setSlop(2)
            .build();

    var expected = new MultiPhraseQuerySpec(FieldPath.parse("foo"), "[bar|baz, quux]", 2);
    var result = MultiPhraseQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryWithEmptyPosition() {
    String field = "$type:string/foo";
    MultiPhraseQuery query =
        new MultiPhraseQuery.Builder()
            .add(new Term[] {new Term(field, "bar"), new Term(field, "baz")})
            .add(new Term[] {})
            .add(new Term(field, "quux"))
            .setSlop(2)
            .build();

    var expected = new MultiPhraseQuerySpec(FieldPath.parse("foo"), "[bar|baz, , quux]", 2);
    var result = MultiPhraseQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
