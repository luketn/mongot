package com.xgen.testing.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.AnalyzerContainer;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Assert;

public class AnalyzerTestUtil {

  private static final AnalyzerRegistry EMPTY_ANALYZER_REGISTRY = AnalyzerRegistryBuilder.empty();

  public static void testAnalyzerShouldProduceToken(Analyzer analyzer, String value)
      throws Exception {
    try (TokenStream stream = analyzer.tokenStream("foo", value)) {
      CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
      stream.reset();

      Assert.assertTrue(stream.incrementToken());
      Assert.assertEquals(value, charTermAttribute.toString());
      Assert.assertFalse(stream.incrementToken());
    }
  }

  public static void testShouldNotProduceToken(Analyzer analyzer, String value) throws Exception {
    try (TokenStream stream = analyzer.tokenStream("foo", value)) {
      stream.reset();
      Assert.assertFalse(stream.incrementToken());
    }
  }

  public static List<String> readStopwordsFromFile(String filename, String commentIdentifier) {
    Path stopwordsPath = Path.of("src/test/unit/resources/index/analyzer", filename + ".txt");

    try {
      return Files.readAllLines(stopwordsPath).stream()
          .filter(line -> !line.startsWith(commentIdentifier))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format(
              "error reading stopwords from file: %s in AnalyzerTestUtil: %s",
              filename, e.getMessage()),
          e);
    }
  }

  public static AnalyzerContainer createAnalyzerContainerOrFail(StockAnalyzerNames analyzerName) {
    return createAnalyzerContainerOrFail(analyzerName.getName());
  }

  public static AnalyzerContainer createAnalyzerContainerOrFail(String analyzerName) {
    return new AnalyzerContainer(
        OverriddenBaseAnalyzerDefinition.stockAnalyzerWithName(analyzerName),
        EMPTY_ANALYZER_REGISTRY.getAnalyzer(analyzerName));
  }

  public static AnalyzerContainer createAnalyzerContainerOrFail(
      AnalyzerDefinition analyzerDefinition) {
    try {
      return AnalyzerContainer.create(analyzerDefinition);
    } catch (InvalidAnalyzerDefinitionException e) {
      throw new IllegalArgumentException("failed trying to create analyzer container", e);
    }
  }
}
