package com.xgen.mongot.catalogservice;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.time.Instant;
import java.util.Arrays;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ServerStateEntryTest.TestDeserialization.class,
      ServerStateEntryTest.TestSerialization.class
    })
public class ServerStateEntryTest {
  private static final String RESOURCE_PATH = "src/test/unit/resources/catalogservice";

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "server-state-deserialization";
    private static final BsonDeserializationTestSuite<ServerStateEntry> TEST_SUITE =
        fromDocument(RESOURCE_PATH, SUITE_NAME, ServerStateEntry::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ServerStateEntry> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<ServerStateEntry> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ServerStateEntry>> data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ServerStateEntry> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          new ServerStateEntry(
              new ObjectId("000003e8464f5e2393000000"),
              "server",
              Instant.parse("2025-12-29T10:00:00Z")));
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "server-state-serialization";
    private static final BsonSerializationTestSuite<ServerStateEntry> TEST_SUITE =
        fromEncodable(RESOURCE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<ServerStateEntry> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<ServerStateEntry> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ServerStateEntry>> data() {
      return Arrays.asList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ServerStateEntry> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          new ServerStateEntry(
              new ObjectId("000003e8464f5e2393000000"),
              "server",
              Instant.parse("2025-12-29T10:00:00Z")));
    }
  }
}
