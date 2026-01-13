package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class LengthTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.LengthTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testKeepsRegularWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.LengthTokenFilter.builder().build();

    var shortStrings = repeat("a", 0, 255);

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, shortStrings, shortStrings);

    // 𠜎 is two UTF-16 code units long
    var doubleLengthChars = repeat("𠜎", 0, 127);

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, doubleLengthChars, doubleLengthChars);
  }

  @Test
  public void testExcludesWords() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(5).max(9).build();

    var shortStrings = repeat("a", 0, 255);
    var inBoundsStrings = repeat("a", 5, 9);

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, shortStrings, inBoundsStrings);

    // € is one UTF-16 code unit long, but three UTF-8 code units long
    var euroChars = repeat("€", 0, 255);
    var expectedEuroChars = repeat("€", 5, 9);

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, euroChars, expectedEuroChars);

    // 𠜎 is two UTF-16 code units long
    var doubleLengthChars = repeat("𠜎", 0, 9);
    var expectedDoubleLengthChars = repeat("𠜎", 3, 4);

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, doubleLengthChars, expectedDoubleLengthChars);
  }

  private static List<String> repeat(String toRepeat, int min, int max) {
    ArrayList<String> strings = new ArrayList<>(max - min);
    for (int i = min; i <= max; i++) {
      strings.add(toRepeat.repeat(i));
    }
    return strings;
  }
}
