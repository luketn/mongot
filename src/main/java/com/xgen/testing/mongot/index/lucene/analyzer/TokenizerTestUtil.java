package com.xgen.testing.mongot.index.lucene.analyzer;

import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import com.xgen.mongot.index.lucene.analyzer.LuceneAnalyzerFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Assert;

public class TokenizerTestUtil {

  /** Tests that tokenizer produces expected tokens. */
  public static void testTokenizerProducesTokens(
      TokenizerDefinition tokenizerDefinition, String input, List<String> output) throws Exception {
    Supplier<Tokenizer> tokenizerSupplier =
        LuceneAnalyzerFactory.TokenizerFactory.build(tokenizerDefinition);

    // Run input through tokenizer twice to test that it is stateless.
    assertTokenizerProducesTokens(
        tokenizerSupplier, input, output, "should produce expected tokens");
    assertTokenizerProducesTokens(
        tokenizerSupplier,
        input,
        output,
        "should produce expected tokens when tokenizing second input");
  }

  private static void assertTokenizerProducesTokens(
      Supplier<Tokenizer> tokenizerSupplier, String input, List<String> output, String message)
      throws Exception {
    Tokenizer tokenizer = tokenizerSupplier.get();

    tokenizer.setReader(new StringReader(input));
    CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);
    tokenizer.reset();

    ArrayList<String> tokens = new ArrayList<>();
    while (tokenizer.incrementToken()) {
      tokens.add(charTermAttribute.toString());
    }
    tokenizer.close();

    Assert.assertEquals(message, output, tokens);
  }
}
