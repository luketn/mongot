package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.information.LongDistanceFeatureQueryBuilder;
import java.util.Optional;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class LatLongDistanceFeatureQuerySpecCreatorTest {
  @Test
  public void testFromQueryInt64() {
    Query query =
        org.apache.lucene.document.LongField.newDistanceFeatureQuery(
            "$type:int64/foo", 1.0f, 42L, 12L);

    // Path and representation are missing here; this is a shortcoming of this query. See
    // LongDistanceFeatureQuery comments for a better description of why it is difficult to infer
    // those values.
    LuceneQuerySpecification expected = LongDistanceFeatureQueryBuilder.builder().build();
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

  @Test
  public void testFromQueryDouble() {
    Query query =
        org.apache.lucene.document.LongField.newDistanceFeatureQuery(
            "$type:double/foo",
            1.0f,
            LuceneDoubleConversionUtils.toLong(9.123E-12),
            LuceneDoubleConversionUtils.toLong(4.2E-13));

    // Path and representation are missing here; this is a shortcoming of this query. See
    // LongDistanceFeatureQuery comments for a better description of why it is difficult to infer
    // those values.
    LuceneQuerySpecification expected = LongDistanceFeatureQueryBuilder.builder().build();
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
