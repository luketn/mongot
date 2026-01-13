package com.xgen.mongot.config.manager.metrics;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.config.manager.metrics.GroupedIndexGenerationMetricsBuilder;
import com.xgen.testing.mongot.config.manager.metrics.IndexGenerationStateMetricsBuilder;
import com.xgen.testing.mongot.index.IndexGenerationMetricsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsBuilder;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.List;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      GroupedIndexGenerationMetricsTest.TestDeserialization.class,
      GroupedIndexGenerationMetricsTest.TestSerialization.class,
    })
public class GroupedIndexGenerationMetricsTest {
  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "grouped-index-generation-metrics-deserialization";
    private static final BsonDeserializationTestSuite<GroupedIndexGenerationMetrics> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/config/manager/metrics",
            SUITE_NAME,
            GroupedIndexGenerationMetrics::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<GroupedIndexGenerationMetrics>
        testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<GroupedIndexGenerationMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<GroupedIndexGenerationMetrics>>
        data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<GroupedIndexGenerationMetrics> simple() {
      ObjectId indexId = new ObjectId("507f191e810c19729de860ea");
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          GroupedIndexGenerationMetricsBuilder.builder()
              .indexId(indexId)
              .indexGenerationStateMetrics(
                  List.of(
                      IndexGenerationStateMetricsBuilder.builder()
                          .state(IndexConfigState.LIVE)
                          .indexGenerationMetrics(
                              IndexGenerationMetricsBuilder.builder()
                                  .generationId(GenerationIdBuilder.create(indexId, 0, 5, 0))
                                  .indexMetrics(IndexMetricsBuilder.sample())
                                  .build())
                          .build()))
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "grouped-index-generation-metrics-serialization";
    private static final BsonSerializationTestSuite<GroupedIndexGenerationMetrics> TEST_SUITE =
        fromEncodable("src/test/unit/resources/config/manager/metrics", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<GroupedIndexGenerationMetrics> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<GroupedIndexGenerationMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<GroupedIndexGenerationMetrics>>
        data() {
      return List.of(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<GroupedIndexGenerationMetrics> simple() {
      ObjectId indexId = new ObjectId("507f191e810c19729de860ea");

      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          GroupedIndexGenerationMetricsBuilder.builder()
              .indexId(indexId)
              .indexGenerationStateMetrics(
                  List.of(
                      IndexGenerationStateMetricsBuilder.builder()
                          .state(IndexConfigState.LIVE)
                          .indexGenerationMetrics(
                              IndexGenerationMetricsBuilder.builder()
                                  .generationId(GenerationIdBuilder.create(indexId, 1, 6, 3))
                                  .indexMetrics(IndexMetricsBuilder.sample())
                                  .build())
                          .build()))
              .build());
    }
  }
}
