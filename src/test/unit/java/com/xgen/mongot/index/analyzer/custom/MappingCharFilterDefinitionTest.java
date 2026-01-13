package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.CharFilterTestUtil;
import org.junit.Test;

public class MappingCharFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("a", "b").build(),
        "",
        "");
  }

  @Test
  public void testDoesNotModifyRegularCharacters() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("a", "b").build(),
        "the lzy fox jumped over the dog or something.!?",
        "the lzy fox jumped over the dog or something.!?");
  }

  @Test
  public void testMapsChars() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("a", "b").build(),
        "the lazy fox jumped over the dog or something.!?",
        "the lbzy fox jumped over the dog or something.!?");
  }

  @Test
  public void testMapsAllChars() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("a", "b").build(),
        "an aadvark and a apple are ardent agents apt and artistic.",
        "bn bbdvbrk bnd b bpple bre brdent bgents bpt bnd brtistic.");
  }

  @Test
  public void testMappingsAreAppliedGreedily() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder()
            .mapping("and", "or")
            .mapping("a", "b")
            .build(),
        "a and b",
        "b or b");

    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder()
            .mapping("a", "b")
            .mapping("and", "or")
            .build(),
        "a and b",
        "b or b");
  }

  @Test
  public void testMappingsAreNotAppliedMoreThanOnce() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder()
            .mapping("and", "or")
            .mapping("orange", "pear")
            .build(),
        "andange",
        "orange");

    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder()
            .mapping("orange", "pear")
            .mapping("ear", "nose")
            .build(),
        "orange",
        "pear");
  }

  @Test
  public void testFrozenMapping() throws Exception {
    CharFilterTestUtil.testCharFilterProducesChars(
        CustomCharFilterDefinitionBuilder.MappingCharFilter.builder().mapping("V", "5").build(),
        "Frozen V",
        "Frozen 5");
  }
}
