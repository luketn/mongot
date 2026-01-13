package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class TokenizerDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "custom-tokenizer-deserialization";
    private static final BsonDeserializationTestSuite<TokenizerDefinition> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            "src/test/unit/resources/index/analyzer/custom",
            SUITE_NAME,
            TokenizerDefinition::fromBson);

    @Parameterized.Parameter
    public BsonDeserializationTestSuite.TestSpecWrapper<TokenizerDefinition> testSpec;

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<TokenizerDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          edgeGram(),
          edgeGramMinMaxEqual(),
          ngram(),
          ngramMinMaxEqual(),
          standard(),
          standardMaxTokenLength(),
          standardSmallMaxTokenLength(),
          standardLargeMaxTokenLength(),
          whitespace(),
          whitespaceMaxTokenLength(),
          whitespaceSmallMaxTokenLength(),
          whitespaceLargeMaxTokenLength(),
          regexSplit(),
          regexSplitEmptyPattern(),
          regexCaptureGroup(),
          regexCaptureGroupEmptyPattern(),
          keyword(),
          uaxUrlEmail());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> standard() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "standard tokenizer", TokenizerDefinitionBuilder.StandardTokenizer.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        standardMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "standard tokenizer with max token length",
          TokenizerDefinitionBuilder.StandardTokenizer.builder().maxTokenLength(123).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        standardSmallMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "standard tokenizer with small max token length",
          TokenizerDefinitionBuilder.StandardTokenizer.builder().maxTokenLength(1).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        standardLargeMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "standard tokenizer with large max token length",
          TokenizerDefinitionBuilder.StandardTokenizer.builder().maxTokenLength(1048576).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> whitespace() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "whitespace tokenizer", TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        whitespaceMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "whitespace tokenizer with max token length",
          TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().maxTokenLength(123).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        whitespaceSmallMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "whitespace tokenizer with small max token length",
          TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().maxTokenLength(1).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        whitespaceLargeMaxTokenLength() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "whitespace tokenizer with large max token length",
          TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().maxTokenLength(1048576).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> regexSplit() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regexSplit tokenizer",
          TokenizerDefinitionBuilder.RegexSplitTokenizer.builder()
              .pattern("\"^\\\\b\\\\d{3}[-.]?\\\\d{3}[-.]?\\\\d{4}\\\\b$\"")
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        regexSplitEmptyPattern() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regexSplit tokenizer with empty pattern",
          TokenizerDefinitionBuilder.RegexSplitTokenizer.builder().pattern("").build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> regexCaptureGroup() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regexCaptureGroup tokenizer",
          TokenizerDefinitionBuilder.RegexCaptureGroupTokenizer.builder()
              .pattern("\"^\\\\b\\\\d{3}[-.]?\\\\d{3}[-.]?\\\\d{4}\\\\b$\"")
              .group(0)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        regexCaptureGroupEmptyPattern() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regexCaptureGroup tokenizer with empty pattern",
          TokenizerDefinitionBuilder.RegexCaptureGroupTokenizer.builder()
              .pattern("")
              .group(0)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> keyword() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "keyword tokenizer", TokenizerDefinitionBuilder.KeywordTokenizer.build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> edgeGram() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "edgeGram",
          TokenizerDefinitionBuilder.EdgeGramTokenizer.builder().minGram(1).maxGram(3).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition>
        edgeGramMinMaxEqual() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "edgeGram min max equal",
          TokenizerDefinitionBuilder.EdgeGramTokenizer.builder().minGram(3).maxGram(3).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> ngram() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nGram",
          TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(1).maxGram(3).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> ngramMinMaxEqual() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nGram min max equal",
          TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(3).maxGram(3).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenizerDefinition> uaxUrlEmail() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "uaxUrlEmail",
          TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().maxTokenLength(200).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "custom-tokenizer-serialization";
    private static final BsonSerializationTestSuite<TokenizerDefinition> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/analyzer/custom", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<TokenizerDefinition> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<TokenizerDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<TokenizerDefinition>> data() {
      return List.of(
          edgeGram(),
          ngram(),
          standard(),
          standardMaxTokenLength(),
          whitespace(),
          whitespaceMaxTokenLength(),
          regexSplit(),
          regexCaptureGroup(),
          keyword(),
          uaxUrlEmail());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> edgeGram() {
      return BsonSerializationTestSuite.TestSpec.create(
          "edgeGram",
          TokenizerDefinitionBuilder.EdgeGramTokenizer.builder().minGram(1).maxGram(3).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> ngram() {
      return BsonSerializationTestSuite.TestSpec.create(
          "nGram",
          TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(1).maxGram(3).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> standard() {
      return BsonSerializationTestSuite.TestSpec.create(
          "standard", TokenizerDefinitionBuilder.StandardTokenizer.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition>
        standardMaxTokenLength() {
      return BsonSerializationTestSuite.TestSpec.create(
          "standard max token length",
          TokenizerDefinitionBuilder.StandardTokenizer.builder().maxTokenLength(123).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> whitespace() {
      return BsonSerializationTestSuite.TestSpec.create(
          "whitespace", TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition>
        whitespaceMaxTokenLength() {
      return BsonSerializationTestSuite.TestSpec.create(
          "whitespace max token length",
          TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().maxTokenLength(123).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> regexSplit() {
      return BsonSerializationTestSuite.TestSpec.create(
          "regexSplit",
          TokenizerDefinitionBuilder.RegexSplitTokenizer.builder()
              .pattern("\"^\\\\b\\\\d{3}[-.]?\\\\d{3}[-.]?\\\\d{4}\\\\b$\"")
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> regexCaptureGroup() {
      return BsonSerializationTestSuite.TestSpec.create(
          "regexCaptureGroup",
          TokenizerDefinitionBuilder.RegexCaptureGroupTokenizer.builder()
              .pattern("\"^\\\\b\\\\d{3}[-.]?\\\\d{3}[-.]?\\\\d{4}\\\\b$\"")
              .group(0)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> keyword() {
      return BsonSerializationTestSuite.TestSpec.create(
          "keyword tokenizer", TokenizerDefinitionBuilder.KeywordTokenizer.build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenizerDefinition> uaxUrlEmail() {
      return BsonSerializationTestSuite.TestSpec.create(
          "uaxUrlEmail", TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().build());
    }
  }

  public static class TestDefinition {

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> TokenizerDefinitionBuilder.StandardTokenizer.builder().build(),
          () -> TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().build(),
          () -> TokenizerDefinitionBuilder.RegexSplitTokenizer.builder().pattern(".*").build(),
          () ->
              TokenizerDefinitionBuilder.RegexCaptureGroupTokenizer.builder()
                  .pattern("(.*)")
                  .group(0)
                  .build(),
          () ->
              TokenizerDefinitionBuilder.EdgeGramTokenizer.builder().minGram(1).maxGram(3).build(),
          () ->
              TokenizerDefinitionBuilder.EdgeGramTokenizer.builder().minGram(2).maxGram(3).build(),
          () ->
              TokenizerDefinitionBuilder.EdgeGramTokenizer.builder().minGram(1).maxGram(4).build(),
          () -> TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(1).maxGram(3).build(),
          () -> TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(2).maxGram(3).build(),
          () -> TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(1).maxGram(4).build(),
          TokenizerDefinitionBuilder.KeywordTokenizer::build);
    }
  }
}
