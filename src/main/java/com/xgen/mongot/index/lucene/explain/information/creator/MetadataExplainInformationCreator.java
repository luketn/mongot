package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.LuceneMetadataExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.MetadataExplainInformation;
import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.OptimizationFlagsDefinition;
import java.util.Optional;

public class MetadataExplainInformationCreator {
  public static MetadataExplainInformation fromFeatureExplainer(
      Optional<String> mongotVersion,
      Optional<String> mongotHostName,
      Optional<String> indexName,
      Optional<CursorOptionsDefinition> cursorOptions,
      Optional<OptimizationFlagsDefinition> optimizationFlagsDefinition,
      Optional<LuceneMetadataExplainInformation> lucene) {
    return new MetadataExplainInformation(
        mongotVersion,
        mongotHostName,
        indexName,
        cursorOptions.map(CursorOptionsDefinition::toBson),
        optimizationFlagsDefinition.map(OptimizationFlagsDefinition::toBson),
        lucene);
  }
}
