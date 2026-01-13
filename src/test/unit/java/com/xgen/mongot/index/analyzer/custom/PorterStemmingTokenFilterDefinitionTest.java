package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class PorterStemmingTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build();

    List<String> list =
        List.of("the", "robot", "can", "eat", "pizza", "a", "byte", "at", "a", "time");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, list);
  }

  @Test
  public void testSameRoot() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build();

    List<String> wordsWithSameRoot = List.of("connect", "connected", "connection", "connecting");

    List<String> removedSuffixes = Collections.nCopies(4, "connect");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, wordsWithSameRoot, removedSuffixes);
  }

  @Test
  public void testKeepsDoubleConsonantEnding() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build();

    List<String> list = List.of("assess", "pass", "gigawatt", "full");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, list);
  }

  @Test
  public void testSuffixes() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build();

    List<String> suffixes = List.of("ance", "ate", "ive", "able");
    List<String> expectedSuffixes = List.of("anc", "at", "iv", "abl");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, suffixes, expectedSuffixes);
  }

  @Test
  public void testEndsWithY() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build();

    List<String> list = List.of("achy", "revolutionary", "happy", "alloy", "alley");
    List<String> expectedList = List.of("achi", "revolutionari", "happi", "alloi", "allei");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }
}
