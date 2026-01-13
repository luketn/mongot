package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.LuceneMetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.creator.MetadataExplainInformationCreator;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.OptimizationFlagsDefinition;
import java.util.Optional;

public class MetadataFeatureExplainer implements FeatureExplainer {
  private Optional<String> mongotVersion;
  private Optional<String> mongotHostName;
  private Optional<String> indexName;
  private Optional<CursorOptionsDefinition> cursorOptions;
  private Optional<OptimizationFlagsDefinition> optimizationFlags;
  private Optional<LuceneMetadataExplainInformation> lucene;

  public MetadataFeatureExplainer() {
    this.mongotVersion = Optional.empty();
    this.mongotHostName = Optional.empty();
    this.indexName = Optional.empty();
    this.cursorOptions = Optional.empty();
    this.optimizationFlags = Optional.empty();
    this.lucene = Optional.empty();
  }

  public void setMongotVersion(String mongotVersion) {
    this.mongotVersion = Optional.of(mongotVersion);
  }

  public void setMongotHostName(String mongotHostName) {
    this.mongotHostName = Optional.of(mongotHostName);
  }

  public void setIndexName(String indexName) {
    this.indexName = Optional.of(indexName);
  }

  public void setCursorOptions(Optional<CursorOptionsDefinition> cursorOptions) {
    this.cursorOptions = cursorOptions;
  }

  public void setOptimizationFlags(Optional<OptimizationFlagsDefinition> optimizationFlags) {
    this.optimizationFlags = optimizationFlags;
  }

  public void setLucene(LuceneMetadataExplainInformation lucene) {
    this.lucene = Optional.of(lucene);
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    builder.metadata(
        MetadataExplainInformationCreator.fromFeatureExplainer(
            this.mongotVersion,
            this.mongotHostName,
            this.indexName,
            this.cursorOptions,
            this.optimizationFlags,
            this.lucene));
  }
}
