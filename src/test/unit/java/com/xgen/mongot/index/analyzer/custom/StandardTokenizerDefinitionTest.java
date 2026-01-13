package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenizerTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class StandardTokenizerDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.StandardTokenizer.builder().build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "", Collections.emptyList());
  }

  @Test
  public void testSimpleWords() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.StandardTokenizer.builder().build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "the brown cow", List.of("the", "brown", "cow"));
  }

  @Test
  public void testMaxTokenLength() throws Exception {
    var tokenizer =
        TokenizerDefinitionBuilder.StandardTokenizer.builder().maxTokenLength(5).build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer,
        "a is the four short medium longest",
        List.of("a", "is", "the", "four", "short", "mediu", "m", "longe", "st"));
  }
}
