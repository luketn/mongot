package com.xgen.mongot.index.lucene.query.custom;

import com.mongodb.lang.Nullable;
import com.xgen.mongot.index.lucene.util.VectorSearchUtil;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Class for executing exact vector search over a vector index. Takes a "Match All" or filter query
 * and overrides the scoring with a vector similarity function to perform a brute force exact vector
 * search.
 */
public class ExactVectorSearchQuery extends Query {
  private final Query filterQuery;
  private final VectorSimilarityFunction similarityFunction;

  private final Vector targetVector;
  private final String field;

  public ExactVectorSearchQuery(
      String field,
      Vector targetVector,
      VectorSimilarityFunction similarityFunction,
      Query filterQuery) {
    this.field = field;
    this.targetVector = targetVector;
    this.similarityFunction = similarityFunction;
    this.filterQuery = filterQuery;
  }

  public Vector getTargetVector() {
    return this.targetVector;
  }

  public Query getFilterQuery() {
    return this.filterQuery;
  }

  public String getField() {
    return this.field;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return this.similarityFunction;
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + ":" + this.field + this.targetVector.toString();
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    Query rewrittenFilter = this.filterQuery.rewrite(searcher);
    if (rewrittenFilter != this.filterQuery) {
      return new ExactVectorSearchQuery(
          this.field, this.targetVector, this.similarityFunction, rewrittenFilter);
    }
    return super.rewrite(searcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight filterQueryWeight =
        searcher.createWeight(this.filterQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);

    return new Weight(this) {
      @Override
      @Nullable
      public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
        Scorer filterScorer = filterQueryWeight.scorer(leafReaderContext);
        // Some filters result with no matches for a term query will return a null scorer.
        // In these cases pass on the null, since TopScoreDocCollector is expecting it.
        if (filterScorer == null) {
          return null;
        }
        return new ExactVectorSearchScorer(
            filterScorer,
            leafReaderContext.reader().getFloatVectorValues(ExactVectorSearchQuery.this.field),
            leafReaderContext.reader().getByteVectorValues(ExactVectorSearchQuery.this.field),
            ExactVectorSearchQuery.this.targetVector,
            ExactVectorSearchQuery.this.similarityFunction);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Scorer filterQueryScorer = filterQueryWeight.scorer(context);
        if (filterQueryScorer == null) {
          return Explanation.noMatch("no filter matches");
        }
        Explanation filterQueryExplanation = filterQueryScorer.getWeight().explain(context, doc);
        if (!filterQueryExplanation.isMatch()) {
          return filterQueryExplanation;
        }

        Scorer s = scorer(context);
        if (s == null) {
          return Explanation.noMatch(getQuery().toString(), filterQueryExplanation);
        }
        return s.iterator().advance(doc) == doc
            ? Explanation.match(
                s.score(), ExactVectorSearchQuery.this.toString(), filterQueryExplanation)
            // should never reach this case, would've terminated at filterQuery match check
            : Explanation.noMatch(getQuery().toString(), filterQueryExplanation);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // matches depend only on filter query, and scoring does not depend on global statistics or
        // docValues, so this query should be fully cacheable
        return filterQueryWeight.isCacheable(ctx);
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    this.filterQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.FILTER, this));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !o.getClass().equals(ExactVectorSearchQuery.class)) {
      return false;
    }
    ExactVectorSearchQuery that = (ExactVectorSearchQuery) o;
    return Objects.equals(this.targetVector, that.targetVector)
        && Objects.equals(this.filterQuery, that.filterQuery)
        && Objects.equals(this.field, that.field)
        && Objects.equals(this.similarityFunction, that.similarityFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.targetVector, this.filterQuery, this.field, this.similarityFunction);
  }

  static class ExactVectorSearchScorer extends FilterScorer {
    private final FloatVectorValues floatVectorValues;
    private final ByteVectorValues byteVectorValues;
    private final Vector target;
    private final VectorSimilarityFunction similarityFunction;

    ExactVectorSearchScorer(
        Scorer filterQueryScorer,
        FloatVectorValues floatVectorValues,
        ByteVectorValues byteVectorValues,
        Vector target,
        VectorSimilarityFunction similarityFunction) {
      super(filterQueryScorer);
      this.floatVectorValues = floatVectorValues;
      this.byteVectorValues = byteVectorValues;
      this.target = target;
      this.similarityFunction = similarityFunction;
    }

    @Override
    public float score() throws IOException {
      Vector.VectorType vectorType = this.target.getVectorType();
      return switch (vectorType) {
        case FLOAT -> {
          if (this.floatVectorValues.advance(docID()) != docID()) {
            throw new AssertionError(
                String.format(
                    "value docID %s should match subquery docID %s",
                    this.floatVectorValues.docID(), docID()));
          }
          yield this.similarityFunction.compare(
              this.floatVectorValues.vectorValue(), this.target.asFloatVector().getFloatVector());
        }
        case BYTE -> {
          if (this.byteVectorValues.advance(docID()) != docID()) {
            throw new AssertionError(
                String.format(
                    "value docID %s should match subquery docID %s",
                    this.byteVectorValues.docID(), docID()));
          }
          yield this.similarityFunction.compare(
              this.byteVectorValues.vectorValue(), this.target.getBytes());
        }
        case BIT -> {
          if (this.byteVectorValues.advance(docID()) != docID()) {
            throw new AssertionError(
                String.format(
                    "value docID %s should match subquery docID %s",
                    this.byteVectorValues.docID(), docID()));
          }
          // similarityFunction.compare() cannot be used here because it assumes each dimension is
          // one byte (instead of one bit as in this case)
          int dimensions = this.target.numDimensions();
          yield VectorSearchUtil.bitSimilarity(
              this.target.getBytes(), this.byteVectorValues.vectorValue(), dimensions);
        }
      };
    }

    @Override
    public float getMaxScore(int upTo) {
      // score should be unreachable from vector similarity functions
      return Float.MAX_VALUE;
    }
  }
}
