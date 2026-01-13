package com.xgen.mongot.index.analyzer;

import static com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames.LUCENE_KUROMOJI;
import static com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames.LUCENE_STANDARD;

import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TokenStreamTypeProviderTest {

  @Test
  public void testMetadataProviderOverriddenAnalyzer() {
    Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers =
        Map.ofEntries(
            streamOverriddenAnalyzer("streamOverridden"),
            graphOverriddenAnalyzer("graphOverridden"));
    Map<String, CustomAnalyzerDefinition> customAnalyzers = Map.of();

    testResult(overriddenAnalyzers, customAnalyzers, "streamOverridden", false);
    testResult(overriddenAnalyzers, customAnalyzers, "graphOverridden", true);
    testThrows(overriddenAnalyzers, customAnalyzers, "unknown");
  }

  @Test
  public void testMetadataProviderCustomAnalyzer() {
    Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers = Map.of();
    Map<String, CustomAnalyzerDefinition> customAnalyzers =
        Map.ofEntries(customGraphAnalyzer("graphCustom"), customStreamAnalyzer("streamCustom"));

    testResult(overriddenAnalyzers, customAnalyzers, "streamCustom", false);
    testResult(overriddenAnalyzers, customAnalyzers, "graphCustom", true);
    testThrows(overriddenAnalyzers, customAnalyzers, "miCüstomAnalyżer");
  }

  @Test
  public void testMetadataProviderStockAnalyzer() {
    Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers = Map.of();
    Map<String, CustomAnalyzerDefinition> customAnalyzers = Map.of();

    testResult(overriddenAnalyzers, customAnalyzers, LUCENE_STANDARD.getName(), false);
    testResult(overriddenAnalyzers, customAnalyzers, LUCENE_KUROMOJI.getName(), true);
    testThrows(overriddenAnalyzers, customAnalyzers, "lucene.nope");
  }

  @Test
  public void testTogether() {
    Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers =
        Map.ofEntries(
            streamOverriddenAnalyzer("streamOverridden"),
            graphOverriddenAnalyzer("graphOverridden"));
    Map<String, CustomAnalyzerDefinition> customAnalyzers =
        Map.ofEntries(customGraphAnalyzer("graphCustom"), customStreamAnalyzer("streamCustom"));

    testResult(overriddenAnalyzers, customAnalyzers, "streamOverridden", false);
    testResult(overriddenAnalyzers, customAnalyzers, "graphOverridden", true);
    testResult(overriddenAnalyzers, customAnalyzers, "graphCustom", true);
    testResult(overriddenAnalyzers, customAnalyzers, "streamCustom", false);
    testThrows(overriddenAnalyzers, customAnalyzers, "miCüstomAnalyżer");
    testThrows(overriddenAnalyzers, customAnalyzers, "");
    testResult(overriddenAnalyzers, customAnalyzers, LUCENE_STANDARD.getName(), false);
    testResult(overriddenAnalyzers, customAnalyzers, LUCENE_KUROMOJI.getName(), true);
    testThrows(overriddenAnalyzers, customAnalyzers, "lucene");
  }

  private static void testThrows(
      Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers,
      Map<String, CustomAnalyzerDefinition> customAnalyzers,
      String name) {
    var target = new TokenStreamTypeProvider(overriddenAnalyzers, customAnalyzers);
    Assert.assertThrows(AssertionError.class, () -> target.isGraphOrThrow(name));
  }

  private static void testResult(
      Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers,
      Map<String, CustomAnalyzerDefinition> customAnalyzers,
      String name,
      boolean expected) {
    boolean actual =
        new TokenStreamTypeProvider(overriddenAnalyzers, customAnalyzers).isGraphOrThrow(name);
    Assert.assertEquals(
        String.format("expected isGraph = %s but got %s", expected, actual), expected, actual);
  }

  private static Map.Entry<String, OverriddenBaseAnalyzerDefinition> streamOverriddenAnalyzer(
      String name) {
    return overriddenAnalyzer(name, LUCENE_STANDARD.getName());
  }

  private static Map.Entry<String, OverriddenBaseAnalyzerDefinition> graphOverriddenAnalyzer(
      String name) {
    return overriddenAnalyzer(name, LUCENE_KUROMOJI.getName());
  }

  private static Map.Entry<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzer(
      String name, String baseAnalyzer) {
    return Map.entry(
        name,
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name(name)
            .baseAnalyzerName(baseAnalyzer)
            .ignoreCase(false)
            .build());
  }

  private static Map.Entry<String, CustomAnalyzerDefinition> customStreamAnalyzer(String name) {
    return customAnalyzer(name, TokenizerDefinitionBuilder.StandardTokenizer.builder().build());
  }

  private static Map.Entry<String, CustomAnalyzerDefinition> customGraphAnalyzer(String name) {
    return customAnalyzer(
        name, TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(2).maxGram(10).build());
  }

  private static Map.Entry<String, CustomAnalyzerDefinition> customAnalyzer(
      String name, TokenizerDefinition tokenizer) {
    return Map.entry(name, CustomAnalyzerDefinitionBuilder.builder(name, tokenizer).build());
  }
}
