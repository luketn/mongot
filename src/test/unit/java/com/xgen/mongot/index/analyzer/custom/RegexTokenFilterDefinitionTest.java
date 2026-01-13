package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class RegexTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("a")
            .replacement("b")
            .matches(RegexTokenFilterDefinition.Matches.ALL)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testMatchesAll() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("a")
            .replacement("b")
            .matches(RegexTokenFilterDefinition.Matches.ALL)
            .build();

    var input = Arrays.asList("a", "aa", "ab", "b", "bb", "ba", "");
    var output = Arrays.asList("b", "bb", "bb", "b", "bb", "bb", "");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, input, output);
  }

  @Test
  public void testMatchesFirst() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("a")
            .replacement("b")
            .matches(RegexTokenFilterDefinition.Matches.FIRST)
            .build();

    var input = Arrays.asList("a", "aa", "ab", "b", "bb", "ba", "");
    var output = Arrays.asList("b", "ba", "bb", "b", "bb", "bb", "");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, input, output);
  }

  @Test
  public void testEmptyPattern() throws Exception {
    TokenFilterDefinition tokenFilterDefinitionAll =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("")
            .replacement("b")
            .matches(RegexTokenFilterDefinition.Matches.ALL)
            .build();
    TokenFilterDefinition tokenFilterDefinitionFirst =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("")
            .replacement("b")
            .matches(RegexTokenFilterDefinition.Matches.FIRST)
            .build();

    var input = Arrays.asList("a", "aa", "ab", "b", "bb", "ba", "");

    var outputAll = Arrays.asList("bab", "babab", "babbb", "bbb", "bbbbb", "bbbab", "b");
    var outputFirst = Arrays.asList("ba", "baa", "bab", "bb", "bbb", "bba", "b");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinitionAll, input, outputAll);
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinitionFirst, input, outputFirst);
  }

  @Test
  public void testEmptyReplaceent() throws Exception {
    TokenFilterDefinition tokenFilterDefinitionAll =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("a")
            .replacement("")
            .matches(RegexTokenFilterDefinition.Matches.ALL)
            .build();
    TokenFilterDefinition tokenFilterDefinitionFirst =
        TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
            .pattern("a")
            .replacement("")
            .matches(RegexTokenFilterDefinition.Matches.FIRST)
            .build();

    var input = Arrays.asList("a", "aa", "ab", "b", "bb", "ba", "");

    var outputAll = Arrays.asList("", "", "b", "b", "bb", "b", "");
    var outputFirst = Arrays.asList("", "a", "b", "b", "bb", "b", "");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinitionAll, input, outputAll);
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinitionFirst, input, outputFirst);
  }
}
