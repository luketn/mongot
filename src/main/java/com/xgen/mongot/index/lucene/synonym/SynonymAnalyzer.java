package com.xgen.mongot.index.lucene.synonym;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;

/**
 * An analyzer configured to apply synonyms at query-time. {@link SynonymAnalyzer} appends a {@link
 * SynonymGraphFilter} to an existing lucene analyzer using {@link AnalyzerWrapper}, a lucene
 * analyzer that provides for easy extensibility of an existing analyzer.
 */
public class SynonymAnalyzer extends AnalyzerWrapper {
  /**
   * {@link SynonymGraphFilter} can be configured to ignore case when comparing query text to
   * user-provided synonyms, but we keep it case-sensitive in order to have consistent behavior when
   * used with lucene.keyword analyzer.
   *
   * <p>Note that the only effect this setting has is to use the lowercase form of query terms as
   * "keys" when looking up synonym "values" in a synonym map, it does not affect indexed terms.
   */
  private static final boolean IGNORE_CASE = false;

  private final Analyzer baseAnalyzer;
  private final SynonymMap synonymMap;

  /** Creates a new SynonymAnalyzer using the ReuseStrategy of the base analyzer. */
  protected SynonymAnalyzer(Analyzer baseAnalyzer, SynonymMap synonymMap) {
    super(baseAnalyzer.getReuseStrategy());
    this.baseAnalyzer = baseAnalyzer;
    this.synonymMap = synonymMap;
  }

  public static SynonymAnalyzer create(Analyzer baseAnalyzer, SynonymMap synonymMap) {
    return new SynonymAnalyzer(baseAnalyzer, synonymMap);
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return this.baseAnalyzer;
  }

  @Override
  protected TokenStreamComponents wrapComponents(
      String fieldName, TokenStreamComponents components) {
    // empty fst throws an error in query builder, do not use SynonymGraphFilter in this case
    if (this.synonymMap.fst == null) {
      return components;
    }
    return new TokenStreamComponents(
        components.getSource(),
        new SynonymGraphFilter(components.getTokenStream(), this.synonymMap, IGNORE_CASE));
  }
}
