package com.xgen.mongot.config.backup;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.List;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      DynamicFeatureFlagJournalTest.TestDeserialization.class,
      DynamicFeatureFlagJournalTest.TestSerialization.class,
    })
public class DynamicFeatureFlagJournalTest {
  @RunWith(Parameterized.class)
  public static class TestDeserialization {

    private static final String SUITE_NAME = "feature-flag-journal-deserialization";
    private static final BsonDeserializationTestSuite<DynamicFeatureFlagJournal> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/config/backup/",
            SUITE_NAME,
            DynamicFeatureFlagJournal::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<DynamicFeatureFlagJournal> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<DynamicFeatureFlagJournal> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<DynamicFeatureFlagJournal>>
        data() {
      return TEST_SUITE.withExamples(empty(), multipleValues());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagJournal> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", new DynamicFeatureFlagJournal(List.of()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<DynamicFeatureFlagJournal>
        multipleValues() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multiple values",
          new DynamicFeatureFlagJournal(
              List.of(
                  new DynamicFeatureFlagConfig(
                      "test-feature-flag-name",
                      DynamicFeatureFlagConfig.Phase.UNSPECIFIED,
                      List.of(),
                      List.of(),
                      0,
                      DynamicFeatureFlagConfig.Scope.UNSPECIFIED),
                  new DynamicFeatureFlagConfig(
                      "test-feature-flag-name2",
                      DynamicFeatureFlagConfig.Phase.CONTROLLED,
                      List.of(new ObjectId("507f1f77bcf86cd799439011")),
                      List.of(new ObjectId("507f1f77bcf86cd799439012")),
                      100,
                      DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER))));
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {

    private static final String SUITE_NAME = "feature-flag-journal-serialization";
    private static final BsonSerializationTestSuite<DynamicFeatureFlagJournal> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/config/backup/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagJournal> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagJournal> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagJournal>> data() {
      return List.of(empty(), multipleValues());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagJournal> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", new DynamicFeatureFlagJournal(List.of()));
    }

    private static BsonSerializationTestSuite.TestSpec<DynamicFeatureFlagJournal> multipleValues() {
      return BsonSerializationTestSuite.TestSpec.create(
          "multiple values",
          new DynamicFeatureFlagJournal(
              List.of(
                  new DynamicFeatureFlagConfig(
                      "test-feature-flag-name",
                      DynamicFeatureFlagConfig.Phase.UNSPECIFIED,
                      List.of(),
                      List.of(),
                      0,
                      DynamicFeatureFlagConfig.Scope.UNSPECIFIED),
                  new DynamicFeatureFlagConfig(
                      "test-feature-flag-name2",
                      DynamicFeatureFlagConfig.Phase.CONTROLLED,
                      List.of(new ObjectId("507f1f77bcf86cd799439011")),
                      List.of(new ObjectId("507f1f77bcf86cd799439012")),
                      100,
                      DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER))));
    }
  }
}
