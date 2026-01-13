package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class StempelTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StempelTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StempelTokenFilter.builder().build();

    List<String> regularWords = List.of("mongodb", "dobranoc", "proszę");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, regularWords, regularWords);
  }

  @Test
  public void testSimpleSentence() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StempelTokenFilter.builder().build();

    List<String> list = List.of("czy", "mówisz", "po", "angielsku");
    List<String> expectedList = List.of("czy", "mówić", "po", "angielskzeć");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testPlurals() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StempelTokenFilter.builder().build();

    List<String> list =
        List.of(
            "chłopcy",
            "rzucali",
            "piłkami",
            "bejsbolowymi",
            "tam",
            "iz",
            "powrotem",
            "między",
            "bazami");
    List<String> expectedList =
        List.of(
            "chłotać", "rzucać", "piłka", "bejsbolowy", "tać", "iz", "powrót", "międyć", "bazami");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }
}
