package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class IcuFoldingTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testFolding() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("À", "A", "à", "Ω"), List.of("a", "a", "a", "ω"));
  }
}
