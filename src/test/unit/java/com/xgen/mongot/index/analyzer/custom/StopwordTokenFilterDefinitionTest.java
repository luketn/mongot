package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class StopwordTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
            .tokens(List.of("is", "the"))
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testSimple() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
            .tokens(List.of("is", "the"))
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("MongoDB", "is", "the", "best"), List.of("MongoDB", "best"));
  }

  @Test
  public void testIgnoreCase() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
            .tokens(List.of("is", "the"))
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("MongoDB", "IS", "The", "best"), List.of("MongoDB", "best"));
  }

  @Test
  public void testIgnoreCaseStopwords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
            .tokens(List.of("IS", "THE"))
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("MongoDB", "is", "The", "best"), List.of("MongoDB", "best"));
  }

  @Test
  public void testIgnoreCaseFalse() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
            .tokens(List.of("is", "the"))
            .ignoreCase(false)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of("MongoDB", "IS", "The", "best"),
        List.of("MongoDB", "IS", "The", "best"));
  }
}
