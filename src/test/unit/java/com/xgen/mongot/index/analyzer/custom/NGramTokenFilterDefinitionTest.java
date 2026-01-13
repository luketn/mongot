package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class NGramTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(2).maxGram(3).build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testNGramOmit() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(2).maxGram(3).build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        Arrays.asList("a", "ab", "abc", "abcd"),
        Arrays.asList("ab", "ab", "abc", "bc", "ab", "abc", "bc", "bcd", "cd"));
  }

  @Test
  public void testNGramInclude() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.NGramTokenFilter.builder()
            .minGram(2)
            .maxGram(3)
            .termNotInBounds(NGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        Arrays.asList("a", "ab", "abc", "abcd"),
        Arrays.asList("a", "ab", "ab", "abc", "bc", "ab", "abc", "bc", "bcd", "cd", "abcd"));
  }
}
