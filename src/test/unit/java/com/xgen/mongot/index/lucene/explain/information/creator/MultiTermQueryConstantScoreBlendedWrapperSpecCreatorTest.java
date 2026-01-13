package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.MultiTermQueryConstantScoreBlendedWrapperSpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.MultiTermQueryConstantScoreWrapperBuilder;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryChildren;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.junit.Assert;
import org.junit.Test;

public class MultiTermQueryConstantScoreBlendedWrapperSpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.TermQuery termQuery =
        new org.apache.lucene.search.TermQuery(new Term("$type:string/foo", "bar"));

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

    MultiTermQueryConstantScoreBlendedWrapperSpec expected =
        MultiTermQueryConstantScoreWrapperBuilder.builder()
            .queries(new ArrayList<>(List.of(termQueryInfo)))
            .build();
    MultiTermQueryConstantScoreBlendedWrapperSpec result =
        MultiTermQueryConstantScoreBlendedWrapperSpecCreator.fromQuery(
            Optional.of(children), Explain.Verbosity.ALL_PLANS_EXECUTION);

    MultiTermQueryConstantScoreBlendedWrapperSpecCreator.fromQuery(
        Optional.of(children), Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
