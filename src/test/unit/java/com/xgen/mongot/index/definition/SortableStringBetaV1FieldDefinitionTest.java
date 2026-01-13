package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SortableStringBetaV1FieldDefinitionBuilder;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      SortableStringBetaV1FieldDefinitionTest.TestDeserialization.class,
      SortableStringBetaV1FieldDefinitionTest.TestSerialization.class,
      SortableStringBetaV1FieldDefinitionTest.TestDefinition.class
    })
public class SortableStringBetaV1FieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {

    private static final String SUITE_NAME = "sortable-string-beta-v1-deserialization";
    private static final BsonDeserializationTestSuite<SortableStringBetaV1FieldDefinition>
        TEST_SUITE =
            fromDocument(
                DefinitionTests.RESOURCES_PATH,
                SUITE_NAME,
                SortableStringBetaV1FieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<SortableStringBetaV1FieldDefinition>
        testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<SortableStringBetaV1FieldDefinition>
            testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<SortableStringBetaV1FieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<SortableStringBetaV1FieldDefinition>
        empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", SortableStringBetaV1FieldDefinitionBuilder.builder().build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {

    private static final String SUITE_NAME = "sortable-string-beta-v1-serialization";
    private static final BsonSerializationTestSuite<SortableStringBetaV1FieldDefinition>
        TEST_SUITE =
            load(
                DefinitionTests.RESOURCES_PATH,
                SUITE_NAME,
                SortableStringBetaV1FieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<SortableStringBetaV1FieldDefinition> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<SortableStringBetaV1FieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<SortableStringBetaV1FieldDefinition>>
        data() {
      return List.of(empty());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<SortableStringBetaV1FieldDefinition>
        empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", SortableStringBetaV1FieldDefinitionBuilder.builder().build());
    }
  }

  public static class TestDefinition {

    @Test
    public void testGetType() {
      SortableStringBetaV1FieldDefinition definition =
          SortableStringBetaV1FieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.SORTABLE_STRING_BETA_V1, definition.getType());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> SortableStringBetaV1FieldDefinitionBuilder.builder().build());
    }
  }
}
