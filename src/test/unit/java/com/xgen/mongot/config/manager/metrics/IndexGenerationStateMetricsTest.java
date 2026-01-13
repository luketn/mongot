package com.xgen.mongot.config.manager.metrics;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
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
      IndexGenerationStateMetricsTest.TestSerialization.class,
      IndexGenerationStateMetricsTest.TestDeserialization.class,
    })
public class IndexGenerationStateMetricsTest {
  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "index-generation-state-metrics-deserialization";
    private static final BsonDeserializationTestSuite<IndexGenerationStateMetrics> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/config/manager/metrics",
            SUITE_NAME,
            IndexGenerationStateMetrics::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexGenerationStateMetrics>
        testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexGenerationStateMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<IndexGenerationStateMetrics>>
        data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexGenerationStateMetrics> simple() {
      ObjectId indexId = new ObjectId("507f191e810c19729de860ea");
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          IndexGenerationStateMetricsBuilder.builder()
              .state(IndexConfigState.LIVE)
              .indexGenerationMetrics(
                  IndexGenerationMetricsBuilder.builder()
                      .generationId(GenerationIdBuilder.create(indexId, 0, 5, 0))
                      .indexMetrics(IndexMetricsBuilder.sample())
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "index-generation-state-metrics-serialization";
    private static final BsonSerializationTestSuite<IndexGenerationStateMetrics> TEST_SUITE =
        fromEncodable("src/test/unit/resources/config/manager/metrics", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexGenerationStateMetrics> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<IndexGenerationStateMetrics> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexGenerationStateMetrics>>
        data() {
      return List.of(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexGenerationStateMetrics> simple() {
      ObjectId indexId = new ObjectId("507f191e810c19729de860ea");

      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          IndexGenerationStateMetricsBuilder.builder()
              .state(IndexConfigState.LIVE)
              .indexGenerationMetrics(
                  IndexGenerationMetricsBuilder.builder()
                      .generationId(GenerationIdBuilder.create(indexId, 0, 6, 0))
                      .indexMetrics(IndexMetricsBuilder.sample())
                      .build())
              .build());
    }
  }
}
