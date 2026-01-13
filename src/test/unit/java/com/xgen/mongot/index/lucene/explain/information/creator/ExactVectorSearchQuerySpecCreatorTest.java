package com.xgen.mongot.index.lucene.explain.information.creator;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.index.lucene.explain.information.ExactVectorSearchQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.lucene.explain.information.ExactVectorSearchQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Assert;
import org.junit.Test;

public class ExactVectorSearchQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    ExactVectorSearchQuery query =
        new ExactVectorSearchQuery(
            "a",
            Vector.fromFloats(new float[] {1, 2, 3, 4}, NATIVE),
            VectorSimilarityFunction.EUCLIDEAN,
            new MatchAllDocsQuery());

    QueryExecutionContextNode filterQueryNode =
        new MockQueryExecutionContextNode(new MatchAllDocsQuery());

    LuceneQuerySpecification expected =
        ExactVectorSearchQueryBuilder.builder()
            .path("a")
            .similarityFunction(VectorSimilarityFunction.EUCLIDEAN)
            .build();

    ExactVectorSearchQuerySpec result =
        ExactVectorSearchQuerySpecCreator.fromQuery(
            query, filterQueryNode, Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
