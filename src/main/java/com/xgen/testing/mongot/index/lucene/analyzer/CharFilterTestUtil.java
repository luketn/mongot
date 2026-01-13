package com.xgen.testing.mongot.index.lucene.analyzer;

import com.xgen.mongot.index.analyzer.custom.CharFilterDefinition;
import com.xgen.mongot.index.lucene.analyzer.AnalysisStep;
import com.xgen.mongot.index.lucene.analyzer.LuceneAnalyzerFactory;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import org.junit.Assert;

public class CharFilterTestUtil {
  /** Tests that char filter produces expected chars. */
  public static void testCharFilterProducesChars(
      CharFilterDefinition charFilterDefinition, String input, String output) throws Exception {
    var analysisStep = LuceneAnalyzerFactory.CharFilterFactory.build(charFilterDefinition);

    // Run input through analysis step twice to test that it is stateless.
    assertAnalysisStepOutputExpected(analysisStep, input, output, "should produce expected output");
    assertAnalysisStepOutputExpected(
        analysisStep, input, output, "should produce expected output when analyzing second input");
  }

  private static void assertAnalysisStepOutputExpected(
      AnalysisStep<Reader> analysisStep, String input, String output, String message)
      throws Exception {
    try (Reader charFilterReader = analysisStep.create(new StringReader(input))) {
      var stringWriter = new StringWriter();
      charFilterReader.transferTo(stringWriter);
      Assert.assertEquals(message, output, stringWriter.toString());
    }
  }
}
