package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenizerTestUtil;
import java.util.List;
import org.junit.Test;

public class KeywordTokenizerDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.KeywordTokenizer.build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "", List.of(""));
  }

  @Test
  public void testSimpleWords() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.KeywordTokenizer.build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "the brown cow", List.of("the brown cow"));
  }
}
