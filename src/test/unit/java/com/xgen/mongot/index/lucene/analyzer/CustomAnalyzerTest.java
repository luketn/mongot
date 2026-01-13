package com.xgen.mongot.index.lucene.analyzer;

import static org.mockito.Mockito.mock;

import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.junit.Assert;
import org.junit.Test;

public class CustomAnalyzerTest {
  private static class Mocks {
    static AnalysisStep<Reader> charFilter(Reader in, Reader out) {
      return inActual -> in.equals(inActual) ? out : null;
    }

    static Supplier<Tokenizer> tokenizerSupplier() {
      return StandardTokenizer::new;
    }

    static AnalysisStep<TokenStream> tokenStream(TokenStream in, TokenStream out) {
      return inActual -> in.equals(inActual) ? out : null;
    }
  }

  @Test
  public void testReaderNoCharFilters() {
    CustomAnalyzer analyzer =
        new CustomAnalyzer(
            Collections.emptyList(), Mocks.tokenizerSupplier(), Collections.emptyList());

    var mockReader = mock(Reader.class);
    Assert.assertEquals(
        "should be same reader", mockReader, analyzer.initReader("foo", mockReader));

    Assert.assertEquals(
        "should be same reader",
        mockReader,
        analyzer.initReaderForNormalization("foo", mockReader));
  }

  @Test
  public void testReaderWithCharFilters() {
    var reader = mock(Reader.class);
    var readerAfterA = mock(Reader.class);
    var readerAfterB = mock(Reader.class);

    var charFilterA = Mocks.charFilter(reader, readerAfterA);
    var charFilterB = Mocks.charFilter(readerAfterA, readerAfterB);

    CustomAnalyzer analyzer =
        new CustomAnalyzer(
            List.of(charFilterA, charFilterB), Mocks.tokenizerSupplier(), Collections.emptyList());

    Assert.assertEquals(
        "should be charFilter reader", readerAfterB, analyzer.initReader("foo", reader));

    Assert.assertEquals(
        "should be charFilter reader",
        readerAfterB,
        analyzer.initReaderForNormalization("foo", reader));
  }

  @Test
  public void testNoTokenFilters() {
    var tokenizer = mock(Tokenizer.class);

    CustomAnalyzer analyzer =
        new CustomAnalyzer(Collections.emptyList(), () -> tokenizer, Collections.emptyList());

    Assert.assertEquals(
        "tokenstream output should be tokenizer",
        tokenizer,
        analyzer.createComponents("foo").getTokenStream());

    var tokenStream = mock(TokenStream.class);
    Assert.assertEquals(
        "normalize should output same tokenstream as input",
        tokenStream,
        analyzer.normalize("foo", tokenStream));
  }

  @Test
  public void testTokenFilters() {

    var tokenizer = mock(Tokenizer.class);
    var tokenStreamAfterTokenFilterA = mock(TokenStream.class);
    var tokenStreamAfterTokenFilterB = mock(TokenStream.class);

    var tokenStreamA = Mocks.tokenStream(tokenizer, tokenStreamAfterTokenFilterA);
    var tokenStreamB =
        Mocks.tokenStream(tokenStreamAfterTokenFilterA, tokenStreamAfterTokenFilterB);

    CustomAnalyzer analyzer =
        new CustomAnalyzer(
            Collections.emptyList(), () -> tokenizer, List.of(tokenStreamA, tokenStreamB));

    Assert.assertEquals(
        "createComponents should output tokenStreamB",
        tokenStreamAfterTokenFilterB,
        analyzer.createComponents("foo").getTokenStream());

    Assert.assertEquals(
        "normalize should transform tokenizer to tokenStreamB",
        tokenStreamAfterTokenFilterB,
        analyzer.normalize("foo", tokenizer));
  }
}
