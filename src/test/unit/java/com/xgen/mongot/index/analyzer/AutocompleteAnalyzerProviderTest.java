package com.xgen.mongot.index.analyzer;

import static com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil.createAnalyzerContainerOrFail;

import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.ListUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Assert;
import org.junit.Test;

public class AutocompleteAnalyzerProviderTest {
  private static final AnalyzerContainer STANDARD_BASE_ANALYZER_CONTAINER =
      createAnalyzerContainerOrFail(StockAnalyzerNames.LUCENE_STANDARD);

  private static Analyzer edgeGram(int minGrams, int maxGrams, boolean foldDiacritics) {
    return edgeGram(minGrams, maxGrams, foldDiacritics, STANDARD_BASE_ANALYZER_CONTAINER);
  }

  private static Analyzer edgeGram(
      int minGrams, int maxGrams, boolean foldDiacritics, AnalyzerContainer analyzerContainer) {
    return new AutocompleteAnalyzerProvider(true)
        .getAnalyzer(
            new AutocompleteAnalyzerSpecification(
                minGrams,
                maxGrams,
                foldDiacritics,
                AutocompleteAnalyzerSpecification.TokenizationStrategy.EDGE_GRAM,
                analyzerContainer));
  }

  private static Analyzer ngram(int minGrams, int maxGrams, boolean foldDiacritics) {
    return ngram(minGrams, maxGrams, foldDiacritics, STANDARD_BASE_ANALYZER_CONTAINER);
  }

  private static Analyzer ngram(
      int minGrams, int maxGrams, boolean foldDiacritics, AnalyzerContainer analyzerContainer) {
    return new AutocompleteAnalyzerProvider(true)
        .getAnalyzer(
            new AutocompleteAnalyzerSpecification(
                minGrams,
                maxGrams,
                foldDiacritics,
                AutocompleteAnalyzerSpecification.TokenizationStrategy.NGRAM,
                analyzerContainer));
  }

  private static Analyzer rightEdgeGram(int minGrams, int maxGrams, boolean foldDiacritics) {
    return rightEdgeGram(minGrams, maxGrams, foldDiacritics, STANDARD_BASE_ANALYZER_CONTAINER);
  }

  private static Analyzer rightEdgeGram(
      int minGrams, int maxGrams, boolean foldDiacritics, AnalyzerContainer analyzerContainer) {
    return new AutocompleteAnalyzerProvider(true)
        .getAnalyzer(
            new AutocompleteAnalyzerSpecification(
                minGrams,
                maxGrams,
                foldDiacritics,
                AutocompleteAnalyzerSpecification.TokenizationStrategy.RIGHT_EDGE_GRAM,
                analyzerContainer));
  }

  @Test
  public void testSimple() throws Exception {
    Analyzer analyzer = edgeGram(1, 6, true);
    String input = "the quick brown";
    List<String> expected = List.of("t", "th", "the", "the ", "the q", "the qu", "quick ", "brown");
    List<String> unexpected = List.of("he qui", "uick");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testShortEdge() throws Exception {
    Analyzer analyzer = edgeGram(1, 1, true);
    String input = "the quick brown";
    List<String> expected = List.of("t", "q", "b");
    List<String> unexpected = List.of("th", "qu");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testLongEdge() throws Exception {
    Analyzer analyzer = edgeGram(12, 12, true);
    String input = "the quick brown fox";
    List<String> expected = List.of("the quick br", "quick brown ", "brown fox");
    List<String> unexpected = List.of();

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testShortWordsEdge() throws Exception {
    Analyzer analyzer = edgeGram(2, 10, true);
    String input = "a is the are for the";
    List<String> expected = List.of("a", "a ", "a is ", "a is the", "is", "is t");
    List<String> unexpected = List.of("he");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testLongWordsEdge() throws Exception {
    Analyzer analyzer = edgeGram(2, 10, true);
    String input = "amphibians traditionally circumvent some eventualities";
    List<String> expected =
        List.of("amphibians", "traditiona", "circumvent", "some event", "eventualit");
    List<String> unexpected = List.of("traditionally", "circumvent s");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testNgram() throws Exception {
    Analyzer analyzer = ngram(1, 2, true);
    String input = "the quick brown fox";
    List<String> expected = List.of("t", "th", "h", "he", "e", "e ");
    List<String> unexpected = List.of("the", "qui");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testShortWordsNgram() throws Exception {
    Analyzer analyzer = ngram(1, 15, true);
    String input = "a is the are for the";
    List<String> expected =
        List.of("a", "a ", "a i", "a is th", "a is the", "is the ar", "s the are");
    List<String> unexpected = List.of();

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testLongWordsNgram() throws Exception {
    Analyzer analyzer = ngram(1, 10, true);
    String input = "amphibians traditionally circumvent some eventualities";
    List<String> expected =
        List.of("a", "am", "amphibians", "mphibians", "ians tradi", "t some eve");
    List<String> unexpected = List.of();

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testShortNgram() throws Exception {
    Analyzer analyzer = ngram(1, 1, true);
    String input = "the quick brown fox";
    List<String> expected = List.of("t", "h", "e", "x");
    List<String> unexpected = List.of("th", " b");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testLongNgram() throws Exception {
    Analyzer analyzer = ngram(12, 12, true);
    String input = "the quick brown fox";
    List<String> expected = List.of("the quick br", "he quick bro", "brown fox");
    List<String> unexpected = List.of("uick");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testRightEdge() throws Exception {
    Analyzer analyzer = rightEdgeGram(1, 6, true);
    String input = "quick the brown";
    List<String> expected =
        List.of("n", "nw", "nwo", "nwor", "nworb", "nworb ", "eht k", "eht kc", "kc");
    List<String> unexpected = List.of("nworb e", "worb");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testShortRightEdge() throws Exception {
    Analyzer analyzer = rightEdgeGram(1, 1, true);
    String input = "the quick brown";
    List<String> expected = List.of("n", "k", "e");
    List<String> unexpected = List.of("nw", "kc", "eh");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testLongRightEdge() throws Exception {
    Analyzer analyzer = rightEdgeGram(12, 12, true);
    String input = "the quick brown fox";
    List<String> expected = List.of("xof nworb kc", "nworb kciuq ", "kciuq eht");
    List<String> unexpected = List.of();

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testShortWordsRightEdge() throws Exception {
    Analyzer analyzer = rightEdgeGram(2, 10, true);
    String input = "a is the are for the";
    List<String> expected =
        List.of(
            "eh",
            "eht",
            "eht r",
            "eht ro",
            "eht rof",
            "eht rof ",
            "eht rof e",
            "eht rof er",
            "eht si a",
            "si a",
            "a");
    List<String> unexpected = List.of("ra", "of");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testLongWordsRightEdge() throws Exception {
    Analyzer analyzer = rightEdgeGram(2, 10, true);
    String input = "amphibians traditionally circumvent some eventualities";
    List<String> expected = List.of("seitilautn", "emos", "tnevmucric", "emos tnevm", "yllanoitid");
    List<String> unexpected = List.of("yllanoitidart", "emos tnevmu");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testDiacritics() throws Exception {
    Analyzer analyzer = edgeGram(3, 5, true);
    String input = "résumé ΜΆΪΟΣ Μάϊος";
    List<String> expected = List.of("resum", "μαιοσ");
    List<String> unexpected = List.of("résumé", "Μάϊος", "ΜΆΪΟΣ");

    runTest(analyzer, input, expected, unexpected);

    List<String> firstGroupTokens = tokensFor(analyzer, "ΜΆΪΟΣ");
    List<String> secondGroupTokens = tokensFor(analyzer, "Μάϊος");

    Assert.assertTrue(
        "token sets for identical words should be the same",
        firstGroupTokens.containsAll(secondGroupTokens)
            && secondGroupTokens.containsAll(firstGroupTokens));
  }

  @Test
  public void testNoDiacritics() throws Exception {
    Analyzer analyzer = edgeGram(4, 6, false);
    String input = "résumé";
    List<String> expected = List.of("résumé");
    List<String> unexpected = List.of("resume");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testKeywordBaseAnalyzer() throws Exception {
    var keywordAnalyzer = createAnalyzerContainerOrFail(StockAnalyzerNames.LUCENE_KEYWORD);
    Analyzer analyzer = edgeGram(1, 6, true, keywordAnalyzer);
    String input = "the quick brown";
    List<String> expected = List.of("t", "th", "the", "the ", "the q", "the qu");
    List<String> unexpected = List.of("he qui", "uick", "quick ", "brown");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testSmartChineseBaseAnalyzer() throws Exception {
    var smartCnAnalyzer = createAnalyzerContainerOrFail(StockAnalyzerNames.LUCENE_SMARTCN);
    Analyzer analyzer = edgeGram(1, 6, true, smartCnAnalyzer);

    // characters － and 。 are considered stopwords by the smartcn analyzer.
    String input = "请给我－杯咖啡。";
    List<String> expected = List.of("请", "请 给", "请 给 我", "杯 咖", "杯 咖啡");
    List<String> unexpected = List.of("我 －", "－", "啡 。", "。");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testCustomAnalyzer() throws Exception {
    CustomAnalyzerDefinition splitOnXCharAnalyzer =
        new CustomAnalyzerDefinitionBuilder(
                "splitOnX",
                TokenizerDefinitionBuilder.RegexSplitTokenizer.builder().pattern("x").build())
            .build();
    var customAnalyzer = createAnalyzerContainerOrFail(splitOnXCharAnalyzer);
    Analyzer analyzer = edgeGram(1, 5, true, customAnalyzer);

    // input string to be split on "x" character
    String input = "a xbcdxefxxg";
    List<String> expected =
        List.of(
            "a", "a ", "a ", "a  b", "a  bc", "b", "bc", "bcd", "bcd e", "e", "ef", "ef ", "ef g");
    List<String> unexpected = List.of("a x", "x", "xg", "xx");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void testCustomAnalyzerTransformationsHappenBeforeAutocompleteTransformation()
      throws Exception {
    CustomAnalyzerDefinition customAnalyzerDefinition =
        new CustomAnalyzerDefinitionBuilder(
                "superAnalyzer",
                TokenizerDefinitionBuilder.StandardTokenizer.builder().maxTokenLength(10).build())
            .tokenFilter(TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(5).build())
            .tokenFilter(TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build())
            .build();
    var customAnalyzer = createAnalyzerContainerOrFail(customAnalyzerDefinition);
    Analyzer analyzer = edgeGram(1, 20, false, customAnalyzer);

    String input = "one called jôhñglôbàlinc";
    List<String> expected =
        List.of(
            // Should emit tokens shorter than 5 characters, given the original token (e.g.
            // "called") is greater than 5 tokens long.
            "c",
            "ca",
            "cal",
            "call",
            "calle",
            "called",
            "called j",
            // Should fold diacritics, as configured in the custom analyzer.
            "joh",
            "john",
            // Should emit single tokens up to 10 characters long.
            "johnglobal");

    List<String> unexpected =
        List.of(
            // "one" is less than 5 characters long, so is removed from the token stream by our
            // custom analyzer.
            "one",
            "one c",
            // No tokens with diacritics; diacritics removed by custom analyzer.
            "jôhñ",
            // The last three characters of "johnglobalinc" will be emitted as an independent token,
            // since the standard tokenizer is configured with max token length 10. "inc" should
            // then be filtered out by the length: 5 token filter.
            "inc",
            "johnglobal i");

    runTest(analyzer, input, expected, unexpected);
  }

  @Test
  public void autocompleteAnalyzer_withTruncateTokensEnabled_limitsTokenCount() throws Exception {
    Analyzer analyzer =
        new AutocompleteAnalyzerProvider.AutocompleteAnalyzer(
            new AutocompleteAnalyzerSpecification(
                1,
                5,
                true,
                AutocompleteAnalyzerSpecification.TokenizationStrategy.NGRAM,
                STANDARD_BASE_ANALYZER_CONTAINER),
            true,
            3);
    String input = "the quick brown";

    // We are guaranteed to have more than 3 tokens if we do not apply LimitTokenCountFilter
    List<String> tokens = tokensFor(analyzer, input);
    assert (tokens.size() == 3);
  }

  /**
   * Runs a test, checking that {@code input} produces {@code expected} tokens and does not produce
   * {@code unexpected} tokens after being analyzed with {@code analyzer}.
   *
   * <p>This method respects cardinality of {@code expected} tokens - if the same token is in {@code
   * expected} multiple times, {@code runTest} will check that the expected token is present
   * multiple times after being analyzed.
   */
  private void runTest(
      Analyzer analyzer, String input, List<String> expected, List<String> unexpected)
      throws Exception {
    List<String> tokens = tokensFor(analyzer, input);

    List<String> missingTokens = ListUtils.subtract(expected, tokens);
    Assert.assertTrue(
        String.format("should contain all expected tokens %s", missingTokens),
        missingTokens.isEmpty());

    List<String> unexpectedTokens = ListUtils.intersection(tokens, unexpected);
    Assert.assertTrue(
        String.format("should not contain unexpected tokens %s", unexpectedTokens),
        unexpectedTokens.isEmpty());
  }

  private List<String> tokensFor(Analyzer analyzer, String input) throws Exception {
    TokenStream stream = analyzer.tokenStream("testFieldName", input);
    CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
    stream.reset();

    ArrayList<String> tokens = new ArrayList<>();
    while (stream.incrementToken()) {
      tokens.add(charTermAttribute.toString());
    }
    stream.close();

    return tokens;
  }
}
