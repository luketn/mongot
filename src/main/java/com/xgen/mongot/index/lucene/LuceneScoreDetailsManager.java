package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.ScoreDetails;
import com.xgen.mongot.index.lucene.query.ScoreDetailsWrappedQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.sandbox.search.TermAutomatonQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LuceneScoreDetailsManager {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneScoreDetailsManager.class);
  private final Query luceneQuery;

  LuceneScoreDetailsManager(Query luceneQuery) {
    this.luceneQuery = luceneQuery;
  }

  /**
   * Catching the {@link NullPointerException} below in {@link IndexSearcher#explain(Query, int)}
   * addresses the following situation that could arise in the future if these steps occurred:
   *
   * <p>We add support to Atlas Search for a new type of query. This query, like compound or
   * embedded queries, can nest other queries, and its weight's explain() method calls the nested
   * query's weight's explain() method.
   *
   * <p>Custom logic is not added for this new query in {@link ScoreDetailsWrappedQuery#wrap(Query)}
   * that would otherwise ensure the query is fully wrapped with {@link ScoreDetailsWrappedQuery}
   * inside-out.
   *
   * <p>A user runs score explain over this query when it nests a synonym query ({@link
   * TermAutomatonQuery.TermAutomatonWeight#explain(org.apache.lucene.index.LeafReaderContext, int)}
   * returns null). Now, instead of crashing with an NPE, it would be caught and a placeholder
   * explanation for the query would be returned instead.
   */
  List<ScoreDetails> getScoreDetails(IndexSearcher indexSearcher, TopDocs topDocs) {
    return Arrays.stream(topDocs.scoreDocs)
        .map(
            scoreDoc -> {
              Explanation placeholder =
                  Explanation.match(scoreDoc.score, this.luceneQuery.toString());
              try {
                ScoreDetailsWrappedQuery scoreDetailsWrappedQuery =
                    ScoreDetailsWrappedQuery.wrap(this.luceneQuery);
                try {
                  return Optional.ofNullable(
                          indexSearcher.explain(scoreDetailsWrappedQuery, scoreDoc.doc))
                      .orElse(placeholder);
                } catch (NullPointerException e) {
                  LOG.atWarn()
                      .addKeyValue("exceptionMessage", e.getMessage())
                      .log(
                          "IndexSearcher.explain raised a NullPointerException when "
                              + "generating score details.");
                  return placeholder;
                }
              } catch (IOException | UnsupportedOperationException e) {
                LOG.warn("Could not generate explanation on query.", e);
                return placeholder;
              }
            })
        .map(LuceneScoreDetailsManager::createScoreDetails)
        .collect(Collectors.toUnmodifiableList());
  }

  private static ScoreDetails createScoreDetails(Explanation explanation) {
    return new ScoreDetails(
        explanation.getValue().floatValue(),
        explanation.getDescription(),
        Arrays.stream(explanation.getDetails())
            .map(LuceneScoreDetailsManager::createScoreDetails)
            .collect(Collectors.toUnmodifiableList()));
  }

  public static Optional<LuceneScoreDetailsManager> getScoreDetailsManagerIfPresent(
      boolean scoreDetails, Query luceneQuery, QueryOptimizationFlags queryOptimizationFlags) {
    return !queryOptimizationFlags.omitSearchDocumentResults() && scoreDetails
        ? Optional.of(new LuceneScoreDetailsManager(luceneQuery))
        : Optional.empty();
  }
}
