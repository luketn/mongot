package com.xgen.mongot.index.lucene;

import com.google.common.truth.Truth;
import java.net.URL;
import org.apache.lucene.analysis.cn.smart.HMMChineseTokenizerFactory;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.morfologik.MorfologikFilter;
import org.apache.lucene.analysis.phonetic.BeiderMorseFilter;
import org.apache.lucene.analysis.stempel.StempelPolishStemFilterFactory;
import org.apache.lucene.backward_codecs.lucene95.Lucene95Codec;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queryparser.charstream.FastCharStream;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Version;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify that lucene-core and lucene-backward-codecs are loaded from our fork (version 9.11.1-1),
 * while other Lucene modules come from upstream (9.11.1).
 *
 * <p>The fork is resolved via a separate maven_install ("lucene_fork") and wired into the main
 * dependency graph via override_targets in deps.bzl. If this test fails, check that configuration.
 */
public class LuceneForkVersionTest {

  @Test
  public void luceneCore_fromFork() {
    String jarPath = jarLocationOf(Version.class);
    Truth.assertThat(jarPath).contains("lucene-core-9.11.1-1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-core-9.11.1.jar");
  }

  @Test
  public void luceneBackwardCodecs_fromFork() {
    String jarPath = jarLocationOf(Lucene95Codec.class);
    Truth.assertThat(jarPath).contains("lucene-backward-codecs-9.11.1-1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-backward-codecs-9.11.1.jar");
  }

  @Test
  public void luceneAnalysisCommon_fromUpstream() {
    String jarPath = jarLocationOf(EnglishAnalyzer.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-common-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-common-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisIcu_fromUpstream() {
    String jarPath = jarLocationOf(ICUNormalizer2CharFilter.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-icu-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-icu-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisKuromoji_fromUpstream() {
    String jarPath = jarLocationOf(JapaneseAnalyzer.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-kuromoji-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-kuromoji-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisMorfologik_fromUpstream() {
    String jarPath = jarLocationOf(MorfologikFilter.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-morfologik-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-morfologik-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisNori_fromUpstream() {
    String jarPath = jarLocationOf(KoreanAnalyzer.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-nori-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-nori-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisPhonetic_fromUpstream() {
    String jarPath = jarLocationOf(BeiderMorseFilter.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-phonetic-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-phonetic-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisSmartcn_fromUpstream() {
    String jarPath = jarLocationOf(HMMChineseTokenizerFactory.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-smartcn-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-smartcn-9.11.1-1.jar");
  }

  @Test
  public void luceneAnalysisStempel_fromUpstream() {
    String jarPath = jarLocationOf(StempelPolishStemFilterFactory.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-stempel-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-stempel-9.11.1-1.jar");
  }

  @Test
  public void luceneExpressions_fromUpstream() {
    String jarPath = jarLocationOf(Expression.class);
    Truth.assertThat(jarPath).contains("lucene-expressions-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-expressions-9.11.1-1.jar");
  }

  @Test
  public void luceneFacet_fromUpstream() {
    String jarPath = jarLocationOf(FacetsConfig.class);
    Truth.assertThat(jarPath).contains("lucene-facet-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-facet-9.11.1-1.jar");
  }

  @Test
  public void luceneHighlighter_fromUpstream() {
    String jarPath = jarLocationOf(Highlighter.class);
    Truth.assertThat(jarPath).contains("lucene-highlighter-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-highlighter-9.11.1-1.jar");
  }

  @Test
  public void luceneJoin_fromUpstream() {
    String jarPath = jarLocationOf(BitSetProducer.class);
    Truth.assertThat(jarPath).contains("lucene-join-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-join-9.11.1-1.jar");
  }

  @Test
  public void luceneMisc_fromUpstream() {
    String jarPath = jarLocationOf(SweetSpotSimilarity.class);
    Truth.assertThat(jarPath).contains("lucene-misc-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-misc-9.11.1-1.jar");
  }

  @Test
  public void luceneQueries_fromUpstream() {
    String jarPath = jarLocationOf(CommonTermsQuery.class);
    Truth.assertThat(jarPath).contains("lucene-queries-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-queries-9.11.1-1.jar");
  }

  @Test
  public void luceneQueryparser_fromUpstream() {
    String jarPath = jarLocationOf(FastCharStream.class);
    Truth.assertThat(jarPath).contains("lucene-queryparser-9.11.1.jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-queryparser-9.11.1-1.jar");
  }

  private static String jarLocationOf(Class<?> clazz) {
    URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
    Assert.assertNotNull("Could not determine code source for " + clazz.getName(), location);
    return location.toString();
  }
}
