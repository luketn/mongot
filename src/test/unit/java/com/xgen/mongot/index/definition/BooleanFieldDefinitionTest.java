package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      BooleanFieldDefinitionTest.TestDeserialization.class,
      BooleanFieldDefinitionTest.TestSerialization.class,
      BooleanFieldDefinitionTest.TestDefinition.class
    })
public class BooleanFieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {

    private static final String SUITE_NAME = "boolean-deserialization";
    private static final BsonDeserializationTestSuite<BooleanFieldDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, BooleanFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<BooleanFieldDefinition> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<BooleanFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<BooleanFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<BooleanFieldDefinition> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", BooleanFieldDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {

    private static final String SUITE_NAME = "boolean-serialization";
    private static final BsonSerializationTestSuite<BooleanFieldDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, BooleanFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<BooleanFieldDefinition> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<BooleanFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<BooleanFieldDefinition>> data() {
      return Arrays.asList(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<BooleanFieldDefinition> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", BooleanFieldDefinitionBuilder.builder().build());
    }
  }

  public static class TestDefinition {
    @Test
    public void testGetType() {
      BooleanFieldDefinition definition = BooleanFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.BOOLEAN, definition.getType());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(() -> BooleanFieldDefinitionBuilder.builder().build());
    }
  }
}
