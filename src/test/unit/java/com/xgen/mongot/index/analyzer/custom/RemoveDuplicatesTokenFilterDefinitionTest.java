package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.List;
import org.junit.Test;

public class RemoveDuplicatesTokenFilterDefinitionTest {

  @Test
  public void incrementToken_emptyInput_producesEmptyOutput() throws Exception {
    var filterDefinition = new RemoveDuplicatesTokenFilterDefinition();

    TokenFilterTestUtil.testTokenFilterProducesTokens(filterDefinition, List.of(), List.of());
  }

  @Test
  public void incrementToken_singleToken_isPreserved() throws Exception {
    var filterDefinition = new RemoveDuplicatesTokenFilterDefinition();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        filterDefinition, List.of("the"), List.of("the"));
  }

  @Test
  public void incrementToken_duplicateInput_remainsUnchanged() throws Exception {
    var filterDefinition = new RemoveDuplicatesTokenFilterDefinition();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        filterDefinition, List.of("the", "the", "the", "cow"), List.of("the", "the", "the", "cow"));
  }

  @Test
  public void incrementToken_duplicateTokenAndPosition_respectsKeyword() throws Exception {
    TokenFilterTestUtil.assertTokenFiltersProduceTokens(
        List.of(
            new KeywordRepeatTokenFilterDefinition(), new PorterStemmingTokenFilterDefinition()),
        List.of("the", "troubled", "runner", "running"),
        List.of("the", "the", "troubled", "troubl", "runner", "runner", "running", "run"));

    TokenFilterTestUtil.assertTokenFiltersProduceTokens(
        List.of(
            new KeywordRepeatTokenFilterDefinition(),
            new PorterStemmingTokenFilterDefinition(),
            new RemoveDuplicatesTokenFilterDefinition()),
        List.of("the", "troubled", "runner", "running"),
        List.of("the", "troubled", "troubl", "runner", "running", "run"));
  }
}
