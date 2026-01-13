package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

@VisibleForTesting
public class LuceneVectorIndexMetricValuesSupplier extends LuceneIndexMetricValuesSupplier {

  public LuceneVectorIndexMetricValuesSupplier(
      Supplier<IndexStatus> indexStatusSupplier,
      IndexBackingStrategy indexBackingStrategy,
      VectorIndexReader indexReader,
      LuceneIndexWriter luceneIndexWriter,
      PerIndexMetricsFactory metricsFactory,
      int indexFeatureVersion,
      boolean isIndexFeatureVersionFourEnabled) {
    super(
        indexStatusSupplier,
        indexBackingStrategy,
        indexReader,
        luceneIndexWriter,
        metricsFactory,
        indexFeatureVersion,
        isIndexFeatureVersionFourEnabled);
  }

  @Override
  public Map<FieldName.TypeField, Double> getNumFieldsPerDatatype() {
    return Collections.emptyMap();
  }

  @Override
  public DocCounts getDocCounts() {
    var numDocs = getNumDocs();
    return new DocCounts(numDocs, getNumLuceneMaxDocs(), getMaxLuceneMaxDocs(), numDocs);
  }
}
