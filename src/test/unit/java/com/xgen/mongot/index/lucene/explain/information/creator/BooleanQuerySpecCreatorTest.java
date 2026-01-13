package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.BooleanQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.BooleanQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryChildren;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.junit.Assert;
import org.junit.Test;

public class BooleanQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.TermQuery termQuery =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

    org.apache.lucene.search.BooleanQuery query =
        new org.apache.lucene.search.BooleanQuery.Builder()
            .add(termQuery, BooleanClause.Occur.SHOULD)
            .build();

    QueryExecutionContextNode termQueryNode = new MockQueryExecutionContextNode(termQuery);
    QueryExplainInformation termQueryInfo =
        QueryExplainInformationCreator.fromNode(
            termQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    QueryChildren<QueryExecutionContextNode> children =
        new MockQueryChildren(
            new ArrayList<>(),
            new ArrayList<>(),
            new ArrayList<>(List.of(termQueryNode)),
            new ArrayList<>());

    BooleanQuerySpec expected =
        BooleanQueryBuilder.builder()
            .should(new ArrayList<>(List.of(termQueryInfo)))
            .minimumShouldMatch(0)
            .build();
    BooleanQuerySpec result =
        BooleanQuerySpecCreator.fromQuery(
            query, Optional.of(children), Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
