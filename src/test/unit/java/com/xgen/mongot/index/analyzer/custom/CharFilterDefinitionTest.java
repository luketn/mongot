package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class CharFilterDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "char-filter-deserialization";
    private static final BsonDeserializationTestSuite<CharFilterDefinition> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            "src/test/unit/resources/index/analyzer/custom",
            SUITE_NAME,
            CharFilterDefinition::fromBson);

    @Parameterized.Parameter
    public BsonDeserializationTestSuite.TestSpecWrapper<CharFilterDefinition> testSpec;

    /** Test data. */
    @Parameterized.Parameters(name = "deserialize: {0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<CharFilterDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          htmlStrip(),
          htmlStripEmptyIgnoredTags(),
          htmlStripNoIgnoredTags(),
          icuNormalize(),
          mapping(),
          persian());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CharFilterDefinition> htmlStrip() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "htmlStrip",
          CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().ignoredTag("a").build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CharFilterDefinition>
        htmlStripEmptyIgnoredTags() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "htmlStrip empty ignoredTags",
          CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CharFilterDefinition>
        htmlStripNoIgnoredTags() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "htmlStrip no ignoredTags",
          CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CharFilterDefinition> icuNormalize() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icuNormalize", CustomCharFilterDefinitionBuilder.IcuNormalizeCharFilter.build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CharFilterDefinition> mapping() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "mapping",
          CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("a", "b").build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<CharFilterDefinition> persian() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "persian", CustomCharFilterDefinitionBuilder.PersianCharFilter.build());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "char-filter-serialization";
    private static final BsonSerializationTestSuite<CharFilterDefinition> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/analyzer/custom", SUITE_NAME);

    @Parameterized.Parameter
    public BsonSerializationTestSuite.TestSpec<CharFilterDefinition> testSpec;

    /** Test data. */
    @Parameterized.Parameters(name = "serialize: {0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<CharFilterDefinition>> data() {
      return List.of(htmlStrip(), icuNormalize(), mapping(), persian());
    }

    private static BsonSerializationTestSuite.TestSpec<CharFilterDefinition> htmlStrip() {
      return BsonSerializationTestSuite.TestSpec.create(
          "htmlStrip",
          CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().ignoredTag("a").build());
    }

    private static BsonSerializationTestSuite.TestSpec<CharFilterDefinition> icuNormalize() {
      return BsonSerializationTestSuite.TestSpec.create(
          "icuNormalize", CustomCharFilterDefinitionBuilder.IcuNormalizeCharFilter.build());
    }

    private static BsonSerializationTestSuite.TestSpec<CharFilterDefinition> mapping() {
      return BsonSerializationTestSuite.TestSpec.create(
          "mapping",
          CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("a", "b").build());
    }

    private static BsonSerializationTestSuite.TestSpec<CharFilterDefinition> persian() {
      return BsonSerializationTestSuite.TestSpec.create(
          "persian", CustomCharFilterDefinitionBuilder.PersianCharFilter.build());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static class TestDefinition {
    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () ->
              CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder()
                  .ignoredTag("a")
                  .build(),
          CustomCharFilterDefinitionBuilder.IcuNormalizeCharFilter::build,
          () ->
              CustomCharFilterDefinitionBuilder.MappingCharFilter.builder()
                  .mapping("a", "b")
                  .build(),
          CustomCharFilterDefinitionBuilder.PersianCharFilter::build);
    }
  }
}
