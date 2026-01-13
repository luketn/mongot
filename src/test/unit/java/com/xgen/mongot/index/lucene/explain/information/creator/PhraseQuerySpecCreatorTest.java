package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.PhraseQuerySpec;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class PhraseQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.PhraseQuery query =
        new org.apache.lucene.search.PhraseQuery(2, "$type:string/foo", "bar", "baz");

    PhraseQuerySpec expected = new PhraseQuerySpec(FieldPath.parse("foo"), "[bar, baz]", 2);
    PhraseQuerySpec result = PhraseQuerySpecCreator.fromQuery(query);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
