package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.lucene.explain.explainers.HighlightFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.highlights.HighlightResolver;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.highlights.Highlight;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.mongot.index.query.operators.Operator;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

public class LuceneHighlighterContext {

  private final HighlightResolver highlightResolver;

  /**
   * The analyzer used for specifying the proper analyzer for highlighting. Contains the
   * index-defined analyzer and mappings specifying the path-defined analyzers for the index.
   */
  private final Analyzer indexAnalyzer;

  public LuceneHighlighterContext(SearchFieldDefinitionResolver resolver, Analyzer indexAnalyzer) {
    this.highlightResolver = HighlightResolver.create(resolver);
    this.indexAnalyzer = indexAnalyzer;
  }

  /** Resolves, validates and returns Highlighter, if it is present. */
  public Optional<LuceneUnifiedHighlighter> getHighlighterIfPresent(
      LuceneIndexSearcher searcher,
      Optional<UnresolvedHighlight> highlight,
      Query luceneQuery,
      Operator operator,
      Optional<ReturnScope> returnScope,
      QueryOptimizationFlags queryOptimizationFlags)
      throws InvalidQueryException {
    if (highlight.isEmpty() || queryOptimizationFlags.omitSearchDocumentResults()) {
      return Optional.empty();
    }

    Highlight resolvedHighlight =
        this.highlightResolver.resolveHighlight(
            highlight.get(),
            searcher.getIndexReader(),
            operator,
            returnScope.map(ReturnScope::path));

    if (Explain.isEnabled()) {
      HighlightFeatureExplainer explainer =
          Explain.getQueryInfo()
              .map(
                  info ->
                      info.getFeatureExplainer(
                          HighlightFeatureExplainer.class,
                          () -> new HighlightFeatureExplainer(resolvedHighlight)))
              .orElseThrow();

      ExplainTimings highlightTimings = explainer.getTimings();
      try (var ignored = highlightTimings.split(ExplainTimings.Type.SETUP_HIGHLIGHT)) {
        return Optional.of(
            getHighlighter(searcher, resolvedHighlight, luceneQuery, Optional.of(explainer)));
      }
    }

    return Optional.of(getHighlighter(searcher, resolvedHighlight, luceneQuery, Optional.empty()));
  }

  private LuceneUnifiedHighlighter getHighlighter(
      LuceneIndexSearcher searcher,
      Highlight resolvedHighlight,
      Query luceneQuery,
      Optional<HighlightFeatureExplainer> explainer)
      throws InvalidQueryException {
    LuceneUnifiedHighlighter unifiedHighlighter =
        LuceneUnifiedHighlighter.create(
            searcher, this.indexAnalyzer, resolvedHighlight, luceneQuery, explainer);

    // Ensure Lucene won't barf while processing terms to highlight if applicable.
    unifiedHighlighter.assertHighlightedTermsValid();

    return unifiedHighlighter;
  }
}
