package com.xgen.mongot.index.analyzer;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import java.util.Arrays;
import java.util.Set;
import org.apache.lucene.analysis.CharArraySet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      OverriddenBaseAnalyzerDefinitionTest.TestDeserialization.class,
      OverriddenBaseAnalyzerDefinitionTest.TestSerialization.class,
      OverriddenBaseAnalyzerDefinitionTest.TestDefinition.class,
    })
public class OverriddenBaseAnalyzerDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {

    private static final String SUITE_NAME = "analyzer-deserialization";
    private static final BsonDeserializationTestSuite<OverriddenBaseAnalyzerDefinition> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/index/analyzer/",
            SUITE_NAME,
            OverriddenBaseAnalyzerDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<OverriddenBaseAnalyzerDefinition>
        testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<OverriddenBaseAnalyzerDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<OverriddenBaseAnalyzerDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          simple(),
          explicitIgnoreCase(),
          explicitMaxTokenLength(),
          explicitStopwords(),
          explicitStemExclusionSet());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<OverriddenBaseAnalyzerDefinition>
        simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<OverriddenBaseAnalyzerDefinition>
        explicitIgnoreCase() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit ignoreCase",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .ignoreCase(false)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<OverriddenBaseAnalyzerDefinition>
        explicitMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit maxTokenLength",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .maxTokenLength(13)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<OverriddenBaseAnalyzerDefinition>
        explicitStopwords() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit stopwords",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .stopword("a")
              .stopword("b")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<OverriddenBaseAnalyzerDefinition>
        explicitStemExclusionSet() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit stemExclusionSet",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .stopword("a")
              .stopword("b")
              .excludeStem("c")
              .excludeStem("d")
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {

    private static final String SUITE_NAME = "analyzer-serialization";
    private static final BsonSerializationTestSuite<OverriddenBaseAnalyzerDefinition> TEST_SUITE =
        fromEncodable("src/test/unit/resources/index/analyzer/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition>>
        data() {

      return Arrays.asList(
          simple(), explicitMaxTokenLength(), explicitStopwords(), explicitStemExclusionSet());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition>
        explicitMaxTokenLength() {
      return BsonSerializationTestSuite.TestSpec.create(
          "explicit maxTokenLength",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .maxTokenLength(13)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition>
        explicitStopwords() {
      return BsonSerializationTestSuite.TestSpec.create(
          "explicit stopwords",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .stopword("a")
              .stopword("b")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<OverriddenBaseAnalyzerDefinition>
        explicitStemExclusionSet() {
      return BsonSerializationTestSuite.TestSpec.create(
          "explicit stemExclusionSet",
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName("bar")
              .stopword("a")
              .stopword("b")
              .excludeStem("c")
              .excludeStem("d")
              .build());
    }
  }

  public static class TestDefinition {

    @Test
    public void testGetName() {
      OverriddenBaseAnalyzerDefinition definition =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .build();

      Assert.assertEquals("foo", definition.name());
    }

    @Test
    public void testGetBaseAnalyzerName() {
      OverriddenBaseAnalyzerDefinition definition =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .build();

      Assert.assertEquals(
          StockAnalyzerNames.LUCENE_STANDARD.getName(), definition.getBaseAnalyzerName());
    }

    @Test
    public void testGetIgnoreCase() {
      OverriddenBaseAnalyzerDefinition definition =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .ignoreCase(true)
              .build();

      Assert.assertTrue(definition.getIgnoreCase());
    }

    @Test
    public void testGetMaxTokenLength() {
      OverriddenBaseAnalyzerDefinition without =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .build();

      Assert.assertTrue(without.getMaxTokenLength().isEmpty());

      OverriddenBaseAnalyzerDefinition with =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .maxTokenLength(13)
              .build();

      Assert.assertTrue(with.getMaxTokenLength().isPresent());
      Assert.assertEquals(13, with.getMaxTokenLength().get().intValue());
    }

    @Test
    public void testGetStopwords() {
      OverriddenBaseAnalyzerDefinition without =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .build();

      Assert.assertTrue(without.getStopwords().isEmpty());

      OverriddenBaseAnalyzerDefinition with =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .stopword("hello")
              .stopword("goodbye")
              .build();

      Assert.assertTrue(with.getStopwords().isPresent());
      Assert.assertEquals(
          new CharArraySet(Set.of("hello", "goodbye"), true), with.getStopwords().get());
    }

    @Test
    public void testGetStemExclusionSet() {
      OverriddenBaseAnalyzerDefinition without =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .build();

      Assert.assertTrue(without.getStemExclusionSet().isEmpty());

      OverriddenBaseAnalyzerDefinition with =
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .name("foo")
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .excludeStem("hello")
              .excludeStem("goodbye")
              .build();

      Assert.assertTrue(with.getStemExclusionSet().isPresent());
      Assert.assertEquals(
          new CharArraySet(Set.of("hello", "goodbye"), true), with.getStemExclusionSet().get());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("baz")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("baz")
                  .baseAnalyzerName("bar")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .ignoreCase(false)
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .maxTokenLength(13)
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .maxTokenLength(14)
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .stopword("a")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .stopword("b")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .stopword("a")
                  .stopword("b")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .excludeStem("a")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .excludeStem("b")
                  .build(),
          () ->
              OverriddenBaseAnalyzerDefinitionBuilder.builder()
                  .name("foo")
                  .baseAnalyzerName("bar")
                  .excludeStem("a")
                  .excludeStem("b")
                  .build());
    }
  }
}
