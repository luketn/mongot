package com.xgen.mongot.index.definition.config;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      IndexDefinitionConfigTest.TestDeserialization.class,
      IndexDefinitionConfigTest.TestSerialization.class,
    })
public class IndexDefinitionConfigTest {

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "index-definition-config-serialization";
    private static final BsonSerializationTestSuite<IndexDefinitionConfig> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/definition/config", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexDefinitionConfig> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<IndexDefinitionConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexDefinitionConfig>> data() {
      return List.of(empty(), withValue(), withZeroValue());
    }

    private static BsonSerializationTestSuite.TestSpec<IndexDefinitionConfig> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", IndexDefinitionConfig.create(Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexDefinitionConfig> withValue() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with value", IndexDefinitionConfig.create(Optional.of(2)));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexDefinitionConfig> withZeroValue() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with zero value", IndexDefinitionConfig.create(Optional.of(0)));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "index-definition-config-deserialization";
    private static final BsonDeserializationTestSuite<IndexDefinitionConfig> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            "src/test/unit/resources/index/definition/config",
            SUITE_NAME,
            IndexDefinitionConfig::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexDefinitionConfig> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexDefinitionConfig> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<IndexDefinitionConfig>>
        data() {
      return TEST_SUITE.withExamples(empty(), withValue(), withZeroValue());
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexDefinitionConfig> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", IndexDefinitionConfig.create(Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexDefinitionConfig> withValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with value", IndexDefinitionConfig.create(Optional.of(2)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexDefinitionConfig> withZeroValue() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with zero value", IndexDefinitionConfig.create(Optional.of(0)));
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }
}
