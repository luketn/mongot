package com.xgen.mongot.index.lucene.query.context;

import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryTimeMappingChecks;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

/** Provides query-time context for building Lucene queries. */
public interface QueryFactoryContext {

  /** Returns the set of checks that enforce query-time mapping consistency */
  QueryTimeMappingChecks getQueryTimeMappingChecks();

  /** Indicates whether the index contains embedded field definitions */
  boolean isIndexWithEmbeddedFields();

  /** Returns whether the index supports {@code $exists} queries */
  boolean supportsFieldExistsQuery();

  /** Provides the {@link Analyzer} used for string fields at query time */
  Analyzer getTokenFieldAnalyzer();

  /**
   * Resolves the vector similarity function configured for a given field.
   *
   * @param fieldPath path of the vector field
   * @param embeddedRoot optional root if the field is inside an embedded document
   * @return the {@link VectorSimilarity} function to use
   * @throws InvalidQueryException if the field mapping is invalid or unsupported
   */
  VectorSimilarity getIndexedVectorSimilarityFunction(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot) throws InvalidQueryException;

  /**
   * Resolves the quantization type configured for a given vector field.
   *
   * @param fieldPath path of the vector field
   * @param embeddedRoot optional root if the field is inside an embedded document
   * @return the {@link VectorQuantization} strategy applied to the field
   * @throws InvalidQueryException if the field mapping is invalid or unsupported
   */
  VectorQuantization getIndexedQuantization(FieldPath fieldPath, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException;
}
