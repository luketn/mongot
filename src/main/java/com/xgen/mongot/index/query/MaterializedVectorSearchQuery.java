package com.xgen.mongot.index.query;

import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Map;
import java.util.Optional;

/**
 * MaterializedVectorSearchQuery is a type safe wrapper of VectorSearchQuery to be used in
 * VectorIndexReader::query method. AutoEmbedding VectorSearchQuery doesn't have queryVector during
 * creation time, so it needs to be materialized to MaterializedVectorSearchQuery for
 * VectorIndexReader to consume.
 */
public record MaterializedVectorSearchQuery(
    VectorSearchQuery vectorSearchQuery,
    Vector vector,
    Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {

  public MaterializedVectorSearchQuery(VectorSearchQuery rawVectorSearchQuery, Vector vector) {
    this(rawVectorSearchQuery, vector, Map.of());
  }

  /**
   * Returns the internal field path by converting user provided query path into internal field path
   * stored in index, return original field path if there is no matching entry in materialized view
   * schema mapping.
   *
   * <p>For example: for user provided query: {'index': 'default', 'path': 'title', 'query': 'xxxx'}
   * and {'autoEmbeddingFieldsMapping': {'title': '_autoEmbed.title'}}, the returned internalPath
   * will be '_autoEmbed.title'.
   */
  public FieldPath internalPath() {
    return this.autoEmbeddingFieldsMapping()
        .getOrDefault(
            this.vectorSearchQuery().criteria().path(), this.vectorSearchQuery().criteria().path());
  }

  public Optional<Vector> queryVector() {
    return Optional.of(vector());
  }

  public Optional<VectorSearchQueryInput> query() {
    return vectorSearchQuery().criteria().query();
  }

  public Optional<VectorSearchFilter> filter() {
    return vectorSearchQuery().criteria().filter();
  }

  public int limit() {
    return vectorSearchQuery().criteria().limit();
  }

  public VectorSearchCriteria.Type getVectorSearchType() {
    return query()
        .map(ignored -> VectorSearchCriteria.Type.AUTO_EMBEDDING)
        .orElse(vectorSearchQuery().criteria().getVectorSearchType());
  }

  public Vector.VectorType queryVectorType() {
    return vector().getVectorType();
  }

  public boolean concurrent() {
    return vectorSearchQuery().concurrent();
  }

  public boolean returnStoredSource() {
    return vectorSearchQuery().returnStoredSource();
  }

  /**
   * Returns a materialized version of the query criteria, ensuring that the {@link
   * VectorSearchCriteria#queryVector()} is always present.
   */
  public VectorSearchCriteria materializedCriteria() {
    VectorSearchCriteria criteria = vectorSearchQuery().criteria();
    return switch (criteria) {
      case ApproximateVectorSearchCriteria approximate ->
          new ApproximateVectorSearchCriteria(
              internalPath(),
              Optional.of(this.vector),
              // Exactly one of queryVector or query can be present
              Optional.empty(),
              approximate.filter(),
              approximate.parentFilter(),
              approximate.limit(),
              approximate.numCandidates(),
              approximate.explainOptions(),
              approximate.returnStoredSource(),
              approximate.embeddedOptions());
      case ExactVectorSearchCriteria exact ->
          new ExactVectorSearchCriteria(
              internalPath(),
              Optional.of(this.vector),
              // Exactly one of queryVector or query can be present
              Optional.empty(),
              exact.filter(),
              exact.parentFilter(),
              exact.limit(),
              exact.returnStoredSource(),
              exact.embeddedOptions());
    };
  }
}
