package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class FlattenGraphTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.FlattenGraphTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepAllWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.FlattenGraphTokenFilter.builder().build();

    List<String> list =
        List.of(
            "these",
            "sentences",
            "should",
            "not",
            "change",
            "but",
            "these",
            "foxes",
            "were",
            "jumping",
            "quickly",
            "over",
            "the",
            "rainbow");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, list);
  }
}
