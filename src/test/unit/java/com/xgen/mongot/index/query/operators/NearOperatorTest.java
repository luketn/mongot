package com.xgen.mongot.index.query.operators;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.GeoPoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class NearOperatorTest {

  private static final String SUITE_NAME = "near";
  private static final BsonDeserializationTestSuite<NearOperator> TEST_SUITE =
      fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, NearOperator::fromBson);

  private final TestSpecWrapper<NearOperator> testSpec;

  public NearOperatorTest(TestSpecWrapper<NearOperator> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<NearOperator>> data() {
    return TEST_SUITE.withExamples(
        dateOrigin(), longOrigin(), doubleOrigin(), geoJsonOrigin(), withBoostScore());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<NearOperator> dateOrigin() {
    return TestSpec.valid(
        "date origin",
        OperatorBuilder.near()
            .path("point")
            .origin(
                new DatePoint(
                    new Calendar.Builder()
                        .setDate(2020, Calendar.APRIL, 3)
                        .setTimeOfDay(13, 13, 13, 0)
                        .setTimeZone(TimeZone.getTimeZone("UTC"))
                        .build()
                        .getTime()))
            .pivot(13)
            .build());
  }

  private static ValidSpec<NearOperator> longOrigin() {
    return TestSpec.valid(
        "long origin",
        OperatorBuilder.near().path("point").origin(new LongPoint(13L)).pivot(26).build());
  }

  private static ValidSpec<NearOperator> doubleOrigin() {
    return TestSpec.valid(
        "double origin",
        OperatorBuilder.near().path("point").origin(new DoublePoint(13d)).pivot(26).build());
  }

  private static ValidSpec<NearOperator> geoJsonOrigin() {
    return TestSpec.valid(
        "GeoJSON origin",
        OperatorBuilder.near()
            .path("point")
            .origin(new GeoPoint(MockGeometries.POINT_A))
            .pivot(13)
            .build());
  }

  private static ValidSpec<NearOperator> withBoostScore() {
    return TestSpec.valid(
        "with boost score",
        OperatorBuilder.near()
            .path("point")
            .origin(new DoublePoint(13d))
            .pivot(26)
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build());
  }
}
