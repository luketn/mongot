package com.xgen.mongot.index.lucene.query.context;

import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorQueryTimeMappingChecks;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

public class VectorQueryFactoryContext implements QueryFactoryContext {

  private final VectorQueryTimeMappingChecks checks;
  private final VectorFieldDefinitionResolver resolver;

  public VectorQueryFactoryContext(VectorFieldDefinitionResolver resolver) {
    this.resolver = resolver;
    this.checks = new VectorQueryTimeMappingChecks(resolver);
  }

  @Override
  public VectorQueryTimeMappingChecks getQueryTimeMappingChecks() {
    return this.checks;
  }

  @Override
  public boolean isIndexWithEmbeddedFields() {
    return false;
  }

  @Override
  public boolean supportsFieldExistsQuery() {
    return this.resolver.getIndexCapabilities().supportsFieldExistsQuery();
  }

  @Override
  public Analyzer getTokenFieldAnalyzer() {
    return new KeywordAnalyzer();
  }

  @Override
  public VectorSimilarity getIndexedVectorSimilarityFunction(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot)
      throws
      InvalidQueryException {
    return resolveVectorFieldSpecification(fieldPath).similarity();
  }

  @Override
  public VectorQuantization getIndexedQuantization(
      FieldPath fieldPath, Optional<FieldPath> embeddedRoot)
      throws
      InvalidQueryException {
    return resolveVectorFieldSpecification(fieldPath).quantization();
  }

  /**
   * Checks if the given field path is an auto-embedding field (TEXT or AUTO_EMBED).
   *
   * @param fieldPath the field path to check
   * @return true if the field is an auto-embedding field, false otherwise
   */
  public boolean isAutoEmbedField(FieldPath fieldPath) {
    return this.resolver.isIndexed(fieldPath, VectorIndexFieldDefinition.Type.TEXT)
        || this.resolver.isIndexed(fieldPath, VectorIndexFieldDefinition.Type.AUTO_EMBED);
  }

  private VectorFieldSpecification resolveVectorFieldSpecification(FieldPath fieldPath)
      throws
      InvalidQueryException {
    return this.resolver
        .getVectorFieldSpecification(fieldPath)
        .orElseThrow(
            () ->
                new InvalidQueryException(
                    String.format("%s is not indexed as a vector field", fieldPath)));
  }
}
