package com.xgen.testing.mongot.index.lucene.analyzer;

import com.xgen.mongot.index.analyzer.custom.TokenFilterDefinition;
import com.xgen.mongot.index.lucene.analyzer.AnalysisStep;
import com.xgen.mongot.index.lucene.analyzer.LuceneAnalyzerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Assert;

public class TokenFilterTestUtil {
  static class MockTokenizer extends Tokenizer {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final Iterator<String> tokenIterator;

    public MockTokenizer(List<String> tokens) {
      this.tokenIterator = tokens.iterator();
    }

    @Override
    public final boolean incrementToken() {
      if (this.tokenIterator.hasNext()) {
        this.termAtt.setEmpty();
        this.termAtt.append(this.tokenIterator.next());
        return true;
      }
      return false;
    }
  }

  /** Tests that token filter produces expected tokens. */
  public static void testTokenFilterProducesTokens(
      TokenFilterDefinition tokenFilterDefinition, List<String> input, List<String> output)
      throws Exception {
    testTokenFilterProducesTokens(
        tokenFilterDefinition, input, output, "should produce expected tokens");
  }

  /** Tests that token filter produces expected tokens. */
  public static void testTokenFilterProducesTokens(
      TokenFilterDefinition tokenFilterDefinition,
      List<String> input,
      List<String> output,
      String message)
      throws Exception {
    var analysisStep = LuceneAnalyzerFactory.TokenFilterFactory.build(tokenFilterDefinition);

    // Run input through analysis step twice to test that it is stateless.
    testAnalysisStepProducesTokens(analysisStep, input, output, message);
    testAnalysisStepProducesTokens(
        analysisStep, input, output, String.format("%s when analyzing second input", message));
  }

  /** Tests that token filter produces expected tokens. */
  private static void testAnalysisStepProducesTokens(
      AnalysisStep<TokenStream> analysisStep,
      List<String> input,
      List<String> output,
      String message)
      throws Exception {
    TokenStream out = analysisStep.create(new MockTokenizer(input));
    CharTermAttribute charTermAttribute = out.addAttribute(CharTermAttribute.class);
    out.reset();

    ArrayList<String> generatedTokens = new ArrayList<>();
    while (out.incrementToken()) {
      generatedTokens.add(charTermAttribute.toString());
    }
    out.close();

    Assert.assertEquals(message, output, generatedTokens);
  }

  /**
   * Asserts that a sequence of {@link TokenFilterDefinition}s produce a given sequence of tokens
   * given an input sequence.
   *
   * <p>This method is similar to {@link #testTokenFilterProducesTokens(TokenFilterDefinition, List,
   * List)} but accepts multiple TokenFilterDefinitions. This is important for testing {@link
   * TokenFilter}s that produce additional attributes that are used by subsequent steps.
   */
  public static void assertTokenFiltersProduceTokens(
      List<TokenFilterDefinition> filters, List<String> input, List<String> output)
      throws Exception {
    var tokenizer = new MockTokenizer(input);

    TokenStream out =
        filters.stream()
            .map(LuceneAnalyzerFactory.TokenFilterFactory::build)
            .reduce(AnalysisStep::andThen)
            .orElseThrow()
            .create(tokenizer);
    CharTermAttribute charTermAttribute = out.addAttribute(CharTermAttribute.class);
    out.reset();

    ArrayList<String> generatedTokens = new ArrayList<>();
    while (out.incrementToken()) {
      generatedTokens.add(charTermAttribute.toString());
    }
    out.close();

    Assert.assertEquals(output, generatedTokens);
  }
}
