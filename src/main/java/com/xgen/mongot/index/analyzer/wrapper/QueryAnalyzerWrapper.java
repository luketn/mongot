package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldName.MultiField;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is similar to {@link org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper}
 * with a couple key differences necessary for the query code path.
 *
 * <ul>
 *   <li>It wraps {@link AnalyzerMeta} instead of simple {@link Analyzer}. This is necessary for
 *       some validation and error messages.
 *   <li>It does not allow invalid StringMultiFieldPaths to default to the fallback analyzer
 *   <li>It exposes the get() function because otherwise this is very difficult to unit test
 * </ul>
 */
public class QueryAnalyzerWrapper extends DelegatingAnalyzerWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(QueryAnalyzerWrapper.class);

  private final SearchIndexDefinition indexDefinition;

  /** The analyzer that should be used if there is no static field mapping. */
  private final AnalyzerMeta defaultAnalyzer;

  /** Precomputed map of lucene field names to their respective analyzers. */
  private final ImmutableMap<String, AnalyzerMeta> fieldAnalyzers;

  /** Precomputed prefix map of lucene field name prefixes and their respective analyzers. */
  private final Optional<DynamicTypeSetPrefixMap<AnalyzerMeta>> maybeDynamicFieldAnalyzers;

  /**
   * Constructs with default analyzer and a map of analyzers to use for specific fields.
   *
   * @param index The {@link SearchIndexDefinition} that generated this analyzer
   * @param defaultAnalyzer Any fields not specifically defined to use a different analyzer will use
   *     the one provided here.
   * @param mappings a Map (Lucene field name to the Analyzer) to be used for those fields
   * @param maybeDynamicTypeSetPrefixMap a prefix map which is used to get the nearest
   *     Analyzer/AnalyzerMeta for a given Lucene field name according to the CDI configuration
   */
  QueryAnalyzerWrapper(
      SearchIndexDefinition index,
      AnalyzerMeta defaultAnalyzer,
      ImmutableMap<String, AnalyzerMeta> mappings,
      Optional<DynamicTypeSetPrefixMap<AnalyzerMeta>> maybeDynamicTypeSetPrefixMap) {
    super(PER_FIELD_REUSE_STRATEGY);
    this.indexDefinition = index;
    this.defaultAnalyzer = defaultAnalyzer;
    this.fieldAnalyzers = mappings;
    this.maybeDynamicFieldAnalyzers = maybeDynamicTypeSetPrefixMap;
  }

  /**
   * This method is called internally by Lucene when this instance is set as an {@link Analyzer} for
   * a {@link org.apache.lucene.search.Query}.
   */
  @Override
  protected Analyzer getWrappedAnalyzer(String luceneFieldName) {
    Optional<AnalyzerMeta> maybeAnalyzerMeta =
        Optional.ofNullable(this.fieldAnalyzers.get(luceneFieldName))
            .or(
                () ->
                    this.maybeDynamicFieldAnalyzers.flatMap(
                        x -> x.getNearestRoot(luceneFieldName)));
    if (maybeAnalyzerMeta.isEmpty()) {
      if (MultiField.isTypeOf(luceneFieldName)) {
        LOG.atWarn()
            .addKeyValue("luceneFieldName", luceneFieldName)
            .log("MultiField was not validated before being passed to Lucene");
        StringMultiFieldPath multiPath = MultiField.getFieldPath(luceneFieldName);

        throw new IllegalArgumentException(
            String.format("%s not found in index: %s", multiPath, this.indexDefinition.getName()));
      }

      if (!FieldName.TypeField.STRING.isTypeOf(luceneFieldName)) {
        LOG.atDebug()
            .addKeyValue("luceneFieldName", luceneFieldName)
            .log("Returning default string analyzer for non-string field");
      }
    }
    return maybeAnalyzerMeta
        .map(AnalyzerMeta::getAnalyzer)
        .orElse(this.defaultAnalyzer.getAnalyzer());
  }

  /**
   * Returns the configured analyzer and its corresponding definition for the field name, or the
   * default analyzer if no such field is specified in the index.
   *
   * @throws InvalidQueryException if the provided path specifies a multi that does not appear in
   *     the IndexDefinition
   */
  public AnalyzerMeta getAnalyzerMeta(StringPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    String luceneFieldName = FieldName.getLuceneFieldNameForStringPath(path, embeddedRoot);

    // If user specifies invalid 'multi' param, throw for instead of having fallback behavior
    if (path.isMultiField()) {
      validateMultiPath(path.asMultiField(), luceneFieldName);
    }
    Optional<AnalyzerMeta> maybeAnalyzerMeta =
        Optional.ofNullable(this.fieldAnalyzers.get(luceneFieldName))
            .or(
                () ->
                    this.maybeDynamicFieldAnalyzers.flatMap(
                        x -> x.getNearestRoot(luceneFieldName)));
    return maybeAnalyzerMeta.orElse(this.defaultAnalyzer);
  }

  /**
   * Throws {@link InvalidQueryException} if the user specifies a multi that does not appear in the
   * index.
   */
  public void validateMultiPath(StringMultiFieldPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    String luceneFieldName = FieldName.getLuceneFieldNameForStringPath(path, embeddedRoot);
    validateMultiPath(path, luceneFieldName);
  }

  /* This overload is used internally to prevent redundant string concatenation  */
  private void validateMultiPath(StringMultiFieldPath path, String luceneFieldName)
      throws InvalidQueryException {
    if (!this.fieldAnalyzers.containsKey(luceneFieldName)
        && this.maybeDynamicFieldAnalyzers.map(x -> x.getNearestRoot(luceneFieldName)).isEmpty()) {
      throw new InvalidQueryException(
          String.format("%s not found in index: %s", path, this.indexDefinition.getName()));
    }
  }

  @Override
  public String toString() {
    return String.format(
        "QueryAnalyzerWrapper(staticFieldAnalyzers=%s, dynamicFieldAnalyzers=%s, default=%s)",
        this.fieldAnalyzers, this.maybeDynamicFieldAnalyzers, this.defaultAnalyzer);
  }
}
