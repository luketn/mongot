package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class ShingleTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
            .minShingleSize(2)
            .maxShingleSize(3)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testShingle() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
            .minShingleSize(2)
            .maxShingleSize(3)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        Arrays.asList("a", "b", "cd", "e", "fgh"),
        Arrays.asList("a b", "a b cd", "b cd", "b cd e", "cd e", "cd e fgh", "e fgh"));
  }
}
