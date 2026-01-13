package com.xgen.mongot.index.analyzer.custom;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamType;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class KeywordRepeatTokenFilterDefinitionTest {

  @Test
  public void incrementToken_emptyInput_producesEmptyOutput() throws Exception {
    var filterDefinition = new KeywordRepeatTokenFilterDefinition();

    TokenFilterTestUtil.testTokenFilterProducesTokens(filterDefinition, List.of(), List.of());
  }

  @Test
  public void incrementToken_singleToken_duplicatesInput() throws Exception {
    var filterDefinition = new KeywordRepeatTokenFilterDefinition();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        filterDefinition, List.of("the brown cow"), List.of("the brown cow", "the brown cow"));
  }

  @Test
  public void incrementToken_tokenizedInput_interleavesDuplicates() throws Exception {
    var filterDefinition = new KeywordRepeatTokenFilterDefinition();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        filterDefinition,
        List.of("the", "brown", "cow"),
        List.of("the", "the", "brown", "brown", "cow", "cow"));
  }

  @DataPoints
  public static List<TokenStreamType> tokenStreamTypes = List.of(TokenStreamType.values());

  @Theory
  public void outputTypeGiven_givenAnyType_returnsGraph(TokenStreamType inputType) {
    TokenStreamType type = new KeywordRepeatTokenFilterDefinition().outputTypeGiven(inputType);

    assertEquals(TokenStreamType.GRAPH, type);
  }
}
