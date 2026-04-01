package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class EnglishMinimalStemmingTokenFilterDefinitionTest {

  @Test
  public void apply_emptyInput_producesNoTokens() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishMinimalStemmingTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void apply_wordsWithoutPluralSuffix_leavesTokensUnchanged() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishMinimalStemmingTokenFilter.builder().build();

    // Words not ending in -s are left unchanged
    List<String> regularWords = List.of("consistency", "regular", "word", "mongodb");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, regularWords, regularWords);
  }

  @Test
  public void apply_pluralSEnding_stripsTrailingS() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishMinimalStemmingTokenFilter.builder().build();

    // Trailing -s is removed
    List<String> plurals = List.of("dogs", "cats", "rainbows");
    List<String> expected = List.of("dog", "cat", "rainbow");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, plurals, expected);

    // -es words: only the trailing s is stripped, leaving the e (e.g. "churches" → "churche")
    List<String> esPlurals = List.of("churches", "boxes");
    List<String> esExpected = List.of("churche", "boxe");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, esPlurals, esExpected);
  }

  @Test
  public void apply_iesEnding_convertsToY() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishMinimalStemmingTokenFilter.builder().build();

    // Words ending in -ies are converted to -y
    List<String> plurals = List.of("armies", "ladies", "parties");
    List<String> expected = List.of("army", "lady", "party");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, plurals, expected);
  }

  @Test
  public void apply_usOrSsEnding_leavesTokensUnchanged() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishMinimalStemmingTokenFilter.builder().build();

    // Words ending in -us or -ss are left unchanged
    List<String> invariants = List.of("status", "virus", "class", "grass");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, invariants, invariants);
  }
}
