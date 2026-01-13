package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class KStemmingTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build();

    List<String> regularWords = List.of("this", "is", "a", "regular", "sentence");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, regularWords, regularWords);
  }

  @Test
  public void testPluralNounsAndAdverbs() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build();

    List<String> list =
        List.of("the", "foxes", "were", "jumping", "quickly", "over", "the", "rainbows");
    List<String> expectedList =
        List.of("the", "fox", "were", "jump", "quick", "over", "the", "rainbow");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testKeepsSuffixes() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build();

    List<String> suffixes = List.of("ance", "ate", "ive", "able");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, suffixes, suffixes);
  }

  @Test
  public void testEndsWithY() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build();

    List<String> list = List.of("achy", "revolutionary", "happy", "alloy", "alley");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, list);
  }
}
