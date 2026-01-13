package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class EnglishPossessiveTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build();

    List<String> regularWords = List.of("jones", "names", "pairs", "tests", "searches");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, regularWords, regularWords);
  }

  @Test
  public void testKeepsPossessivePronouns() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build();

    List<String> possessivePronouns =
        List.of("mine", "ours", "yours", "his", "hers", "its", "theirs");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, possessivePronouns, possessivePronouns);
  }

  @Test
  public void testPossessivePluralNouns() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build();

    List<String> list = List.of("jones'", "smiths'", "children's");
    List<String> expectedList = List.of("jones'", "smiths'", "children");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testPluralNouns() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build();

    List<String> list = List.of("brothers", "brother's");
    List<String> expectedList = List.of("brothers", "brother");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testSingularPossessiveNouns() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build();

    List<String> list = List.of("San", "Francisco's", "FISHERMAN'S", "wharf's", "location");
    List<String> expectedList = List.of("San", "Francisco", "FISHERMAN", "wharf", "location");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }
}
