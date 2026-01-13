package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class WordDelimiterGraphTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .generateWordParts(true)
                    .build())
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .generateWordParts(true)
                    .build())
            .build();

    List<String> regularWords = List.of("this", "is", "a", "regular", "sentence");

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, regularWords, regularWords);
  }

  @Test
  public void testProtectedWordsCaseInSensitive() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .generateWordParts(true)
                    .build())
            .protectedWords(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.ProtectedWords.builder()
                    .words(List.of("SIGN-IN", "year-end", "WI-fi"))
                    .build())
            .build();

    List<String> list =
        List.of("just", "sign-in", "for", "your", "year-end", "free-for-all", "wi-fi");
    List<String> expectedList =
        List.of("just", "sign-in", "for", "your", "year-end", "free", "for", "all", "wi-fi");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testProtectedWordsCaseSensitive() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .generateWordParts(true)
                    .build())
            .protectedWords(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.ProtectedWords.builder()
                    .words(List.of("SIGN-IN", "year-end", "WI-fi"))
                    .ignoreCase(false)
                    .build())
            .build();

    List<String> list =
        List.of("just", "SIGN-IN", "for", "your", "year-end", "free-for-all", "wi-fi");
    List<String> expectedList =
        List.of("just", "SIGN-IN", "for", "your", "year-end", "free", "for", "all", "wi", "fi");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testGenerateWordAndNumberParts() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .generateWordParts(true)
                    .generateNumberParts(true)
                    .build())
            .build();

    List<String> list = List.of("1-22", "0", "422-1", "wi-fi");
    List<String> expectedList = List.of("1", "22", "0", "422", "1", "wi", "fi");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testGenerateNumberParts() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .generateNumberParts(true)
                    .build())
            .build();

    List<String> list = List.of("1-22", "0", "422-1");
    List<String> expectedList = List.of("1", "22", "0", "422", "1");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testCatenateWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .concatenateWords(true)
                    .build())
            .build();

    List<String> list =
        List.of("just", "SIGN-IN", "for", "your", "year-end", "free-for-all", "wi-fi", "wi-fi-400");
    List<String> expectedList =
        List.of("just", "SIGNIN", "for", "your", "yearend", "freeforall", "wifi", "wifi");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testCatenateNumbers() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .concatenateNumbers(true)
                    .build())
            .build();

    List<String> list = List.of("1-22", "0", "422-1", "000-00000", "wi-fi-4000");
    List<String> expectedList = List.of("122", "0", "4221", "00000000", "4000");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testCatenateAll() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .concatenateAll(true)
                    .build())
            .build();

    List<String> list = List.of("wifi-1-22");
    List<String> expectedList = List.of("wifi122");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, expectedList);
  }

  @Test
  public void testPreserveOriginal() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
            .delimiterOptions(
                TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                    .allFalse()
                    .preserveOriginal(true)
                    .build())
            .build();

    List<String> list = List.of("1-22", "0", "422-1", "000-00000");

    TokenFilterTestUtil.testTokenFilterProducesTokens(tokenFilterDefinition, list, list);
  }
}
