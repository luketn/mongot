package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class AsciiFoldingTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.AsciiFoldingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testAdd() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.AsciiFoldingTokenFilter.builder()
            .originalTokens(OriginalTokens.INCLUDE)
            .build();

    /*
     * These sample inputs/outputs are from the lucene tests for the AsciiFoldingFilter.
     *
     * <p>See
     * https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/test/org/apache/lucene/analysis/miscellaneous/TestASCIIFoldingFilter.java
     */
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of(
            "Des", "mot", "clés", "À", "LA", "CHAÎNE", "ÀÁÂÃÄÅ", "Æ", "Ç", "ÈÉÊË", "ÌÍÎÏ", "Ĳ", "Ð",
            "Ñ", "ÒÓÔÕÖØ", "Œ", "Þ", "ÙÚÛÜ", "ÝŸ", "àáâãäå", "æ", "ç", "èéêë", "ìíîï", "ĳ", "ð",
            "ñ", "òóôõöø", "œ", "ß", "þ", "ùúûü", "ýÿ", "ﬁ", "ﬂ"),
        List.of(
            "Des", "mot", "cles", "clés", "A", "À", "LA", "CHAINE", "CHAÎNE", "AAAAAA", "ÀÁÂÃÄÅ",
            "AE", "Æ", "C", "Ç", "EEEE", "ÈÉÊË", "IIII", "ÌÍÎÏ", "IJ", "Ĳ", "D", "Ð", "N", "Ñ",
            "OOOOOO", "ÒÓÔÕÖØ", "OE", "Œ", "TH", "Þ", "UUUU", "ÙÚÛÜ", "YY", "ÝŸ", "aaaaaa",
            "àáâãäå", "ae", "æ", "c", "ç", "eeee", "èéêë", "iiii", "ìíîï", "ij", "ĳ", "d", "ð", "n",
            "ñ", "oooooo", "òóôõöø", "oe", "œ", "ss", "ß", "th", "þ", "uuuu", "ùúûü", "yy", "ýÿ",
            "fi", "ﬁ", "fl", "ﬂ"));
  }

  @Test
  public void testReplace() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.AsciiFoldingTokenFilter.builder()
            .originalTokens(OriginalTokens.OMIT)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of(
            "Des", "mot", "clés", "À", "LA", "CHAÎNE", "ÀÁÂÃÄÅ", "Æ", "Ç", "ÈÉÊË", "ÌÍÎÏ", "Ĳ", "Ð",
            "Ñ", "ÒÓÔÕÖØ", "Œ", "Þ", "ÙÚÛÜ", "ÝŸ", "àáâãäå", "æ", "ç", "èéêë", "ìíîï", "ĳ", "ð",
            "ñ", "òóôõöø", "œ", "ß", "þ", "ùúûü", "ýÿ", "ﬁ", "ﬂ"),
        List.of(
            "Des", "mot", "cles", "A", "LA", "CHAINE", "AAAAAA", "AE", "C", "EEEE", "IIII", "IJ",
            "D", "N", "OOOOOO", "OE", "TH", "UUUU", "YY", "aaaaaa", "ae", "c", "eeee", "iiii", "ij",
            "d", "n", "oooooo", "oe", "ss", "th", "uuuu", "yy", "fi", "fl"));
  }
}
