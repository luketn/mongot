package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.ConstantScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.ConstantScoreQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import org.apache.lucene.index.Term;
import org.junit.Assert;
import org.junit.Test;

public class ConstantScoreQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.TermQuery termQuery =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

    org.apache.lucene.search.ConstantScoreQuery query =
        new org.apache.lucene.search.ConstantScoreQuery(termQuery);

    QueryExecutionContextNode termQueryNode = new MockQueryExecutionContextNode(termQuery);
    QueryExplainInformation termQueryInfo =
        QueryExplainInformationCreator.fromNode(
            termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    ConstantScoreQuerySpec expected =
        ConstantScoreQueryBuilder.builder().query(termQueryInfo).build();
    ConstantScoreQuerySpec result =
        ConstantScoreQuerySpecCreator.fromQuery(
            query, termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
