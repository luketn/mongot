package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.DisjunctionMaxQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.DisjunctionMaxQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryChildren;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class DisjunctionMaxQuerySpecCreatorTest {

  @Test
  public void testFromQuery() {
    // Create two TermQueries
    TermQuery termQuery1 = new TermQuery(new Term("field1", "value1"));
    TermQuery termQuery2 = new TermQuery(new Term("field2", "value2"));

    // Create a DisjunctionMaxQuery with these TermQueries
    List<org.apache.lucene.search.Query> disjuncts = Arrays.asList(termQuery1, termQuery2);
    float tieBreaker = 0.1f;
    DisjunctionMaxQuery query = new DisjunctionMaxQuery(disjuncts, tieBreaker);

    // Create MockQueryExecutionContextNodes for the TermQueries
    QueryExecutionContextNode termQueryNode1 = new MockQueryExecutionContextNode(termQuery1);
    QueryExecutionContextNode termQueryNode2 = new MockQueryExecutionContextNode(termQuery2);

    // Create QueryExplainInformation for each node
    QueryExplainInformation termQueryInfo1 =
        QueryExplainInformationCreator.fromNode(
            termQueryNode1, Explain.Verbosity.ALL_PLANS_EXECUTION);
    QueryExplainInformation termQueryInfo2 =
        QueryExplainInformationCreator.fromNode(
            termQueryNode2, Explain.Verbosity.ALL_PLANS_EXECUTION);

    // Create MockQueryChildren
    QueryChildren children =
        new MockQueryChildren(
            new ArrayList<>(),
            new ArrayList<>(),
            Arrays.asList(termQueryNode1, termQueryNode2),
            new ArrayList<>());

    // Build the expected DisjunctionMaxQuerySpec
    DisjunctionMaxQuerySpec expected =
        DisjunctionMaxQueryBuilder.builder()
            .disjuncts(Arrays.asList(termQueryInfo1, termQueryInfo2))
            .tieBreaker(tieBreaker)
            .build();

    // Call the DisjunctionMaxQuerySpecCreator
    DisjunctionMaxQuerySpec result =
        DisjunctionMaxQuerySpecCreator.fromQuery(
            query, Optional.of(children), Explain.Verbosity.ALL_PLANS_EXECUTION);

    // Assert that the result matches the expected DisjunctionMaxQuerySpec
    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
