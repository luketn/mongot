package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.GeoPoint;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

public class NearQueryFactoryTest {

  private static final String PATH_START = "start";

  private static final DatePoint DATE_FIRST =
      new DatePoint(
          new Calendar.Builder()
              .setDate(2019, Calendar.SEPTEMBER, 21)
              .setTimeOfDay(22, 15, 27, 37)
              .setTimeZone(TimeZone.getTimeZone("GMT"))
              .build()
              .getTime());

  private static final DocumentFieldDefinition MAPPED_TO_GEO =
      DocumentFieldDefinitionBuilder.builder()
          .dynamic(false)
          .field(
              "geo",
              FieldDefinitionBuilder.builder()
                  .geo(GeoFieldDefinitionBuilder.builder().indexShapes(true).build())
                  .build())
          .build();

  @Test
  public void testDateNearQuery() throws Exception {
    NearOperator definition =
        OperatorBuilder.near().path(PATH_START).origin(DATE_FIRST).pivot(42).build();

    Query expected =
        LongField.newDistanceFeatureQuery("$type:date/start", 1f, DATE_FIRST.value().getTime(), 42);

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testDateNearQueryMultiplePath() throws Exception {
    NearOperator definition =
        OperatorBuilder.near()
            .path("start")
            .path("end")
            .path("somethingElse")
            .origin(DATE_FIRST)
            .pivot(42)
            .build();

    Query expected =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    LongField.newDistanceFeatureQuery(
                        "$type:date/start", 1f, DATE_FIRST.value().getTime(), 42),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    LongField.newDistanceFeatureQuery(
                        "$type:date/end", 1f, DATE_FIRST.value().getTime(), 42),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    LongField.newDistanceFeatureQuery(
                        "$type:date/somethingElse", 1f, DATE_FIRST.value().getTime(), 42),
                    BooleanClause.Occur.SHOULD))
            .build();

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testDateNearQueryWithBoost() throws Exception {
    NearOperator definition =
        OperatorBuilder.near()
            .path(PATH_START)
            .score(ScoreBuilder.valueBoost().value(1.45f).build())
            .origin(DATE_FIRST)
            .pivot(12)
            .build();

    Query expected =
        new BoostQuery(
            LongField.newDistanceFeatureQuery(
                "$type:date/start", 1f, DATE_FIRST.value().getTime(), 12),
            1.45f);

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testDateNearQueryThrowsWithZeroPivot() {
    NearOperator definition =
        OperatorBuilder.near().path(PATH_START).origin(DATE_FIRST).pivot(0).build();

    Assert.assertThrows(
        IllegalArgumentException.class, () -> LuceneSearchTranslation.get().translate(definition));
  }

  @Test
  public void testDateNearQueryThrowsWithNegativePivot() {
    NearOperator definition =
        OperatorBuilder.near().path(PATH_START).origin(DATE_FIRST).pivot(-1).build();

    Assert.assertThrows(
        IllegalArgumentException.class, () -> LuceneSearchTranslation.get().translate(definition));
  }

  @Test
  public void testNearGeoPoint() throws Exception {
    NearOperator definition =
        OperatorBuilder.near()
            .path("geo")
            .origin(new GeoPoint(MockGeometries.POINT_A))
            .pivot(123)
            .build();

    var expected = LatLonPoint.newDistanceFeatureQuery("$type:geoPoint/geo", 1f, 12.0, 10.0, 123.0);
    LuceneSearchTranslation.mapped(MAPPED_TO_GEO).assertTranslatedTo(definition, expected);
  }

  @Test
  public void testOriginOutOfRangeThrowsInvalidQueryException() {
    // TODO(CLOUDP-280897): move bound validations from lucene factory to operators?
    NearOperator definition =
        OperatorBuilder.near()
            .path("geo")
            .origin(new GeoPoint(MockGeometries.OUT_OF_BOUNDS_POINT))
            .pivot(123)
            .build();
    LuceneSearchTranslation.mapped(MAPPED_TO_GEO).assertTranslationThrows(definition);
  }

  @Test
  public void testNearGeoPointNotMappedToGeoThrowsException() {
    // with dynamic mapping:
    NearOperator definition =
        OperatorBuilder.near()
            .path("geo")
            .origin(new GeoPoint(MockGeometries.POINT_A))
            .pivot(123)
            .build();
    LuceneSearchTranslation.get().assertTranslationThrows(definition);

    // with static mapping but incorrect field:
    NearOperator definition2 =
        OperatorBuilder.near()
            .path("address.location")
            .origin(new GeoPoint(MockGeometries.POINT_A))
            .pivot(123)
            .build();
    LuceneSearchTranslation.mapped(MAPPED_TO_GEO).assertTranslationThrows(definition2);
  }
}
