package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenizerTestUtil;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class NGramTokenizerDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    var tokenizer =
        TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(1).maxGram(2).build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "", Collections.emptyList());
  }

  @Test
  public void testNGram() throws Exception {
    TokenizerDefinition tokenizer =
        TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(2).maxGram(3).build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer,
        "abcdef",
        Arrays.asList("ab", "abc", "bc", "bcd", "cd", "cde", "de", "def", "ef"));
  }
}
