package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.LatLonPointDistanceQueryBuilder;
import java.util.Optional;
import org.apache.lucene.document.LatLonPoint;
import org.junit.Assert;
import org.junit.Test;

public class LatLonPointDistantQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.Query query =
        LatLonPoint.newDistanceQuery("$type:geoPoint/foo", 2, 3, 4);

    // This is not implemented yet, so we expect it to show as a DefaultQuery.
    LuceneQuerySpecification expected = LatLonPointDistanceQueryBuilder.builder().build();
    Optional<LuceneQuerySpecification> result =
        LuceneQuerySpecificationCreator.tryCreate(
            query, Optional.empty(), Explain.Verbosity.ALL_PLANS_EXECUTION);

    Assert.assertTrue("result should be present", result.isPresent());
    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result.get(),
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
