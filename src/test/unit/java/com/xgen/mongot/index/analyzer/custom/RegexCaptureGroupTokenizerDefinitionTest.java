package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenizerTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RegexCaptureGroupTokenizerDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    var tokenizer =
        TokenizerDefinitionBuilder.RegexCaptureGroupTokenizer.builder()
            .pattern("(a.)")
            .group(0)
            .build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "", Collections.emptyList());
  }

  @Test
  public void testSimpleRegexAndGroup() throws Exception {
    var tokenizer =
        TokenizerDefinitionBuilder.RegexCaptureGroupTokenizer.builder()
            .pattern("(b.)(a.)")
            .group(2)
            .build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "b aa bb ab ba", List.of("aa", "ab"));
  }
}
