package com.xgen.mongot.replication.mongodb;

import com.xgen.mongot.util.Bytes;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.util.MockRuntimeBuilder;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {DurabilityConfigTest.TestSerialization.class, DurabilityConfigTest.TestConfig.class})
public class DurabilityConfigTest {

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "durability-config-serialization";
    private static final BsonSerializationTestSuite<DurabilityConfig> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/replication/mongodb", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<DurabilityConfig> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<DurabilityConfig> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<DurabilityConfig>> data() {
      return Collections.singletonList(fullConfig());
    }

    private static BsonSerializationTestSuite.TestSpec<DurabilityConfig> fullConfig() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full config",
          DurabilityConfig.create(Optional.of(10), Optional.of(Duration.ofMinutes(1))));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static class TestConfig {
    @Test
    public void testCommitIntervalDefault() {
      var config = DurabilityConfig.create(Optional.empty(), Optional.empty());
      Assert.assertEquals(Duration.ofMinutes(1), config.commitInterval);
    }

    @Test
    public void testNumCommittingThreadsDefault() {
      var runtime =
          new MockRuntimeBuilder().withNumCpus(8).withMaxHeapSize(Bytes.ofGibi(1)).build();
      var config = DurabilityConfig.create(runtime, Optional.empty(), Optional.empty());
      Assert.assertEquals(4, config.numCommittingThreads);
    }

    @Test
    public void testNumCommittingThreadsDefaultLimit() {
      var runtime =
          new MockRuntimeBuilder().withNumCpus(64).withMaxHeapSize(Bytes.ofGibi(1)).build();
      var config = DurabilityConfig.create(runtime, Optional.empty(), Optional.empty());
      Assert.assertEquals(10, config.numCommittingThreads);
    }
  }
}
