package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.BoostQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.BoostQuerySpecBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.junit.Assert;
import org.junit.Test;

public class BootQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.TermQuery termQuery =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

    org.apache.lucene.search.BoostQuery query = new BoostQuery(termQuery, 42.0f);

    QueryExecutionContextNode termQueryNode = new MockQueryExecutionContextNode(termQuery);
    QueryExplainInformation termQueryInfo =
        QueryExplainInformationCreator.fromNode(
            termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    BoostQuerySpec expected =
        BoostQuerySpecBuilder.builder().query(termQueryInfo).boost(42.0f).build();

    BoostQuerySpec result =
        BoostQuerySpecCreator.fromQuery(
            query, termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
