package com.xgen.mongot.index.lucene.explain.information.creator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.lucene.explain.information.FunctionScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.FunctionScoreQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DoubleValuesSource;
import org.junit.Assert;
import org.junit.Test;

public class FunctionScoreQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.TermQuery termQuery =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

    DoubleValuesSource valuesSource = mock(DoubleValuesSource.class);
    when(valuesSource.toString()).thenReturn("a + b + c");

    org.apache.lucene.queries.function.FunctionScoreQuery query =
        new org.apache.lucene.queries.function.FunctionScoreQuery(termQuery, valuesSource);

    QueryExecutionContextNode termQueryNode = new MockQueryExecutionContextNode(termQuery);
    QueryExplainInformation termQueryInfo =
        QueryExplainInformationCreator.fromNode(
            termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    FunctionScoreQuerySpec expected =
        FunctionScoreQueryBuilder.builder().query(termQueryInfo).scoreFunction("a + b + c").build();
    FunctionScoreQuerySpec result =
        FunctionScoreQuerySpecCreator.fromQuery(
            query, termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
