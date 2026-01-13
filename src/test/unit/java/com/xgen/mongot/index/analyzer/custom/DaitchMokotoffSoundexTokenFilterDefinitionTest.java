package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class DaitchMokotoffSoundexTokenFilterDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DaitchMokotoffSoundexTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testAdd() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DaitchMokotoffSoundexTokenFilter.builder()
            .originalTokens(OriginalTokens.INCLUDE)
            .build();

    /*
     * These sample inputs/outputs are from the lucene tests for the DaitchMokotoffSoundexFilter.
     *
     * @see <a
     *     href=https://github.com/apache/lucene-solr/blob/master/lucene/analysis/phonetic/src/test/org/apache/lucene/analysis/phonetic/TestDaitchMokotoffSoundexFilter.java#L31>TestDaitchMokotoffSoundexFilter</a>
     */
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of("aaa", "bbb", "ccc", "easgasg"),
        List.of(
            "aaa", "000000", "bbb", "700000", "ccc", "400000", "450000", "454000", "540000",
            "545000", "500000", "easgasg", "045450"));
  }

  @Test
  public void testReplace() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DaitchMokotoffSoundexTokenFilter.builder()
            .originalTokens(OriginalTokens.OMIT)
            .build();

    /*
     * These sample inputs/outputs are from the lucene tests for the DaitchMokotoffSoundexFilter.
     *
     * @see <a
     *     href=https://github.com/apache/lucene-solr/blob/master/lucene/analysis/phonetic/src/test/org/apache/lucene/analysis/phonetic/TestDaitchMokotoffSoundexFilter.java#L31>TestDaitchMokotoffSoundexFilter</a>
     */
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition,
        List.of("aaa", "bbb", "ccc", "easgasg"),
        List.of(
            "000000", "700000", "400000", "450000", "454000", "540000", "545000", "500000",
            "045450"));
  }
}
