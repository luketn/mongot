package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenizerTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RegexSplitTokenizerDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.RegexSplitTokenizer.builder().pattern(" ").build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "", Collections.emptyList());
  }

  @Test
  public void testSimpleWords() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.RegexSplitTokenizer.builder().pattern(" ").build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "the brown cow", List.of("the", "brown", "cow"));
  }

  @Test
  public void testDifferentPattern() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.RegexSplitTokenizer.builder().pattern("/").build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "the/brown cow", List.of("the", "brown cow"));
  }
}
