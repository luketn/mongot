package com.xgen.mongot.index.lucene.explain.information.creator;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.xgen.mongot.index.lucene.explain.information.PointRangeQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.Representation;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import java.util.Date;
import java.util.Optional;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class PointRangeQuerySpecCreatorTest {
  @Test
  public void testFromQueryInt64() {
    Query query =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            FieldName.TypeField.NUMBER_INT64.getLuceneFieldName(
                FieldPath.parse("longValPath"), Optional.empty()),
            0,
            10);

    PointRangeQuerySpec expected =
        new PointRangeQuerySpec(
            FieldPath.parse("longValPath"),
            Optional.of(Representation.INT_64),
            Optional.of(new LongPoint(0L)),
            Optional.of(new LongPoint(10L)));

    assertThat(query).isInstanceOf(PointRangeQuery.class);

    PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

    PointRangeQuerySpec result = PointRangeQuerySpecCreator.fromQuery(pointRangeQuery);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryDouble() {
    Query query =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            FieldName.TypeField.NUMBER_DOUBLE.getLuceneFieldName(
                FieldPath.parse("doubleValuePath"), Optional.empty()),
            LuceneDoubleConversionUtils.toLong(-8.203),
            LuceneDoubleConversionUtils.toLong(9.1E42));

    PointRangeQuerySpec expected =
        new PointRangeQuerySpec(
            FieldPath.parse("doubleValuePath"),
            Optional.of(Representation.DOUBLE),
            Optional.of(new DoublePoint(-8.203)),
            Optional.of(new DoublePoint(9.1E42)));

    assertThat(query).isInstanceOf(PointRangeQuery.class);

    PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

    PointRangeQuerySpec result = PointRangeQuerySpecCreator.fromQuery(pointRangeQuery);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryInt64Multiple() {
    Query query =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            FieldName.TypeField.NUMBER_INT64_MULTIPLE.getLuceneFieldName(
                FieldPath.parse("longMultipleValPath"), Optional.empty()),
            0,
            10);

    PointRangeQuerySpec expected =
        new PointRangeQuerySpec(
            FieldPath.parse("longMultipleValPath"),
            Optional.of(Representation.INT_64),
            Optional.of(new LongPoint(0L)),
            Optional.of(new LongPoint(10L)));

    assertThat(query).isInstanceOf(PointRangeQuery.class);

    PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

    PointRangeQuerySpec result = PointRangeQuerySpecCreator.fromQuery(pointRangeQuery);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryDoubleMultiple() {
    Query query =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(
                FieldPath.parse("doubleMultipleValuePath"), Optional.empty()),
            LuceneDoubleConversionUtils.toLong(-8.203),
            LuceneDoubleConversionUtils.toLong(9.1E42));

    PointRangeQuerySpec expected =
        new PointRangeQuerySpec(
            FieldPath.parse("doubleMultipleValuePath"),
            Optional.of(Representation.DOUBLE),
            Optional.of(new DoublePoint(-8.203)),
            Optional.of(new DoublePoint(9.1E42)));

    assertThat(query).isInstanceOf(PointRangeQuery.class);

    PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

    PointRangeQuerySpec result = PointRangeQuerySpecCreator.fromQuery(pointRangeQuery);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryDateNoRepresentation() {
    Query query =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            FieldName.TypeField.DATE.getLuceneFieldName(
                FieldPath.parse("datePath"), Optional.empty()),
            new Date(Long.MAX_VALUE).getTime(),
            new Date(Long.MIN_VALUE).getTime());

    PointRangeQuerySpec expected =
        new PointRangeQuerySpec(
            FieldPath.parse("datePath"),
            Optional.empty(),
            Optional.of(new DatePoint(new Date(Long.MAX_VALUE))),
            Optional.of(new DatePoint(new Date(Long.MIN_VALUE))));

    assertThat(query).isInstanceOf(PointRangeQuery.class);

    PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

    PointRangeQuerySpec result = PointRangeQuerySpecCreator.fromQuery(pointRangeQuery);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }

  @Test
  public void testFromQueryDateMultipleNoRepresentation() {
    Query query =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(
                FieldPath.parse("datePathMultiple"), Optional.empty()),
            new Date(Long.MAX_VALUE).getTime(),
            new Date(Long.MIN_VALUE).getTime());

    PointRangeQuerySpec expected =
        new PointRangeQuerySpec(
            FieldPath.parse("datePathMultiple"),
            Optional.empty(),
            Optional.of(new DatePoint(new Date(Long.MAX_VALUE))),
            Optional.of(new DatePoint(new Date(Long.MIN_VALUE))));

    assertWithMessage("expected query should be instance of PointRangeQuery")
        .that(query)
        .isInstanceOf(PointRangeQuery.class);

    PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

    PointRangeQuerySpec result = PointRangeQuerySpecCreator.fromQuery(pointRangeQuery);

    Assert.assertTrue(
        "result should be as expected",
        expected.equals(
            result,
            ExplainInformationTestUtil.QueryExplainInformationEquator.equator(),
            ExplainInformationTestUtil.totalOrderComparator()));
  }
}
