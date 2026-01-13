package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.Comparators.SCORE_DOC_RELEVANCE_COMPARATOR;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Executes a Lucene {@link Query} once, caches the {@link TopDocs}, and serves subsequent 'getMore'
 * requests from memory.
 *
 * <p>Used in vector search to avoid re-running the same query when iterating through result
 * batches.
 */
public class LuceneCachingVectorSearchManager extends LuceneVectorSearchManager {

  @Var private Optional<TopDocs> hits = Optional.empty();

  public LuceneCachingVectorSearchManager(
      Query luceneQuery,
      VectorSearchCriteria criteria,
      Optional<BinaryQuantizedVectorRescorer> rescorer) {
    super(luceneQuery, criteria, rescorer);
  }

  @Override
  public QueryInfo initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException {
    TopDocs result = super.initialSearch(searcherReference, batchSize).topDocs;

    this.hits = Optional.of(result);
    return new QueryInfo(result, result.scoreDocs.length < batchSize);
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize) {
    if (this.hits.isEmpty()) {
      throw new IllegalStateException("Can't be called before the initial search");
    }

    var initialResult = this.hits.get();

    int lastDocIdx =
        Arrays.binarySearch(initialResult.scoreDocs, lastScoreDoc, SCORE_DOC_RELEVANCE_COMPARATOR);

    int fromIdx = lastDocIdx == initialResult.scoreDocs.length ? lastDocIdx : lastDocIdx + 1;
    return new TopDocs(
        initialResult.totalHits,
        Arrays.copyOfRange(initialResult.scoreDocs, fromIdx, initialResult.scoreDocs.length));
  }
}
