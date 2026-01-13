package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;

/**
 * Version of {@link LuceneOperatorSearchManager} which overrides the batchSize with {@link
 * VectorSearchQuery.Fields#NUM_CANDIDATES} and caps Lucene results using the {@link
 * VectorSearchQuery.Fields#LIMIT}.
 */
public class LuceneVectorSearchManager implements LuceneSearchManager<QueryInfo> {

  private final Query luceneQuery;
  private final VectorSearchCriteria criteria;
  private final Optional<BinaryQuantizedVectorRescorer> rescorer;

  public LuceneVectorSearchManager(
      Query luceneQuery,
      VectorSearchCriteria criteria,
      Optional<BinaryQuantizedVectorRescorer> rescorer) {

    this.rescorer = rescorer;
    this.luceneQuery = luceneQuery;
    this.criteria = criteria;
  }

  @Override
  public QueryInfo initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException {

    int hitsToCollect =
        this.rescorer.isPresent()
            // rescoring needs to operate on the full set of candidates
            ? ((ApproximateVectorSearchCriteria) this.criteria).numCandidates()
            : this.criteria.limit();

    LuceneIndexSearcher indexSearcher = searcherReference.getIndexSearcher();
    TopScoreDocCollectorManager collectorManager =
        new TopScoreDocCollectorManager(hitsToCollect, 0);

    TopDocs topCandidates = topCandidates(indexSearcher, collectorManager);
    TopDocs result =
        new TopDocs(
            // override number of hits to match the limit
            new TotalHits(topCandidates.scoreDocs.length, TotalHits.Relation.EQUAL_TO),
            topCandidates.scoreDocs);

    // explicitly mark results as exhausted, as current limits on numCandidates
    // guarantee that all results fit in a single batch
    return new QueryInfo(result, true);
  }

  private TopDocs topCandidates(
      LuceneIndexSearcher indexSearcher, TopScoreDocCollectorManager collectorManager)
      throws IOException {
    TopDocs topCandidates = indexSearcher.search(this.luceneQuery, collectorManager);

    if (this.rescorer.isEmpty()) {
      return topCandidates;
    }

    Query unwrapped =
        (this.luceneQuery instanceof WrappedKnnQuery wrappedKnnQuery)
            ? wrappedKnnQuery.getQuery()
            : this.luceneQuery;

    return this.rescorer.get().rescore(
        indexSearcher,
        topCandidates,
        (ApproximateVectorSearchCriteria) this.criteria,
        Check.instanceOf(unwrapped, KnnFloatVectorQuery.class));
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize) {
    // the cursor is always marked as exhausted after the initial search,
    // so the getMore call is unexpected
    return Check.unreachable("getMoreTopDocs should not be called for vector search");
  }

  @VisibleForTesting
  public Query getLuceneQuery() {
    return this.luceneQuery;
  }
}
