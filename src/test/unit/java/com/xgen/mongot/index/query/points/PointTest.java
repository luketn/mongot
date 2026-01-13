package com.xgen.mongot.index.query.points;

import static com.xgen.testing.BsonDeserializationTestSuite.fromValue;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      PointTest.PointSuite.class,
      PointTest.DatePointSuite.class,
      PointTest.NumericPointSuite.class,
      PointTest.LongPointSuite.class,
      PointTest.DoublePointSuite.class,
      PointTest.GeoPointSuite.class,
    })
public class PointTest {

  @RunWith(Parameterized.class)
  public static class PointSuite {

    private static final BsonDeserializationTestSuite<Point> TEST_SUITE =
        fromValue("src/test/unit/resources/index/query/points/", "point", Point::fromBson);

    private final TestSpecWrapper<Point> testSpec;

    public PointSuite(TestSpecWrapper<Point> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Point>> data() {
      return TEST_SUITE.withExamples(
          truePoint(),
          falsePoint(),
          dateValue(),
          int32Value(),
          int64Value(),
          doubleValue(),
          geoJsonPointValue(),
          uuidPointValue(),
          geoJsonPointWithIntegralPositions(),
          stringJsonPointValue());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> dateValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "date value",
          new DatePoint(
              new Calendar.Builder()
                  .setDate(2020, Calendar.APRIL, 3)
                  .setTimeOfDay(13, 13, 13, 0)
                  .setTimeZone(TimeZone.getTimeZone("UTC"))
                  .build()
                  .getTime()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> int32Value() {
      return BsonDeserializationTestSuite.TestSpec.valid("int32 value", new LongPoint(13L));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> int64Value() {
      return BsonDeserializationTestSuite.TestSpec.valid("int64 value", new LongPoint(13L));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> doubleValue() {
      return BsonDeserializationTestSuite.TestSpec.valid("double value", new DoublePoint(13d));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> geoJsonPointValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "GeoJSON point value", new GeoPoint(MockGeometries.POINT_A));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> uuidPointValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "UUID point value",
          new UuidPoint(UUID.fromString("00000000-0000-0000-0000-000000000000")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point>
        geoJsonPointWithIntegralPositions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "GeoJSON point with integral positions", new GeoPoint(MockGeometries.POINT_A));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> stringJsonPointValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "string value", new StringPoint("Hello World!"));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> truePoint() {
      return BsonDeserializationTestSuite.TestSpec.valid("true value", new BooleanPoint(true));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> falsePoint() {
      return BsonDeserializationTestSuite.TestSpec.valid("false value", new BooleanPoint(false));
    }
  }

  @RunWith(Parameterized.class)
  public static class DatePointSuite {
    private static final BsonDeserializationTestSuite<Point> TEST_SUITE =
        fromValue("src/test/unit/resources/index/query/points/", "date-point", DatePoint::fromBson);

    private final TestSpecWrapper<Point> testSpec;

    public DatePointSuite(TestSpecWrapper<Point> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Point>> data() {
      return TEST_SUITE.withExamples(dateValue());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> dateValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "date value",
          new DatePoint(
              new Calendar.Builder()
                  .setDate(2020, Calendar.APRIL, 3)
                  .setTimeOfDay(13, 13, 13, 0)
                  .setTimeZone(TimeZone.getTimeZone("UTC"))
                  .build()
                  .getTime()));
    }
  }

  @RunWith(Parameterized.class)
  public static class NumericPointSuite {
    private static final BsonDeserializationTestSuite<Point> TEST_SUITE =
        fromValue(
            "src/test/unit/resources/index/query/points/", "numeric-point", NumericPoint::fromBson);

    private final TestSpecWrapper<Point> testSpec;

    public NumericPointSuite(TestSpecWrapper<Point> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Point>> data() {
      return TEST_SUITE.withExamples(int32Value(), int64Value(), doubleValue());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> int32Value() {
      return BsonDeserializationTestSuite.TestSpec.valid("int32 value", new LongPoint(13L));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> int64Value() {
      return BsonDeserializationTestSuite.TestSpec.valid("int64 value", new LongPoint(13L));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> doubleValue() {
      return BsonDeserializationTestSuite.TestSpec.valid("double value", new DoublePoint(13d));
    }
  }

  @RunWith(Parameterized.class)
  public static class LongPointSuite {
    private static final BsonDeserializationTestSuite<Point> TEST_SUITE =
        fromValue("src/test/unit/resources/index/query/points/", "long-point", LongPoint::fromBson);

    private final TestSpecWrapper<Point> testSpec;

    public LongPointSuite(TestSpecWrapper<Point> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Point>> data() {
      return TEST_SUITE.withExamples(int32Value(), int64Value());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> int32Value() {
      return BsonDeserializationTestSuite.TestSpec.valid("int32 value", new LongPoint(13L));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> int64Value() {
      return BsonDeserializationTestSuite.TestSpec.valid("int64 value", new LongPoint(13L));
    }
  }

  @RunWith(Parameterized.class)
  public static class DoublePointSuite {
    private static final BsonDeserializationTestSuite<Point> TEST_SUITE =
        fromValue(
            "src/test/unit/resources/index/query/points/", "double-point", DoublePoint::fromBson);

    private final TestSpecWrapper<Point> testSpec;

    public DoublePointSuite(TestSpecWrapper<Point> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Point>> data() {
      return TEST_SUITE.withExamples(doubleValue());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> doubleValue() {
      return BsonDeserializationTestSuite.TestSpec.valid("double value", new DoublePoint(13d));
    }
  }

  @RunWith(Parameterized.class)
  public static class GeoPointSuite {
    private static final BsonDeserializationTestSuite<Point> TEST_SUITE =
        fromValue("src/test/unit/resources/index/query/points/", "geo-point", GeoPoint::fromBson);

    private final TestSpecWrapper<Point> testSpec;

    public GeoPointSuite(TestSpecWrapper<Point> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestSpecWrapper<Point>> data() {
      return TEST_SUITE.withExamples(geoJsonPointValue(), geoJsonPointWithIntegralPositions());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point> geoJsonPointValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "GeoJSON point value", new GeoPoint(MockGeometries.POINT_A));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Point>
        geoJsonPointWithIntegralPositions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "GeoJSON point with integral positions", new GeoPoint(MockGeometries.POINT_A));
    }
  }
}
