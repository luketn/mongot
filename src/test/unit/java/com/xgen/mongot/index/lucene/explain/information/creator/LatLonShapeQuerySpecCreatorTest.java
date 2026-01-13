package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.LatLonShapeQueryBuilder;
import java.util.Optional;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Circle;
import org.junit.Assert;
import org.junit.Test;

public class LatLonShapeQuerySpecCreatorTest {
  @Test
  public void testFromQuery() {
    org.apache.lucene.search.Query query =
        LatLonShape.newGeometryQuery(
            "$type:geoShape/foo", ShapeField.QueryRelation.CONTAINS, new Circle(2, 3, 4));

    LuceneQuerySpecification expected = LatLonShapeQueryBuilder.builder().build();
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
