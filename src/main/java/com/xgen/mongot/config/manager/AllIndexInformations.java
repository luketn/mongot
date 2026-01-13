package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.manager.metrics.IndexGenerationStateMetrics;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.util.CollectionUtils;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;

record AllIndexInformations(
    List<IndexInformationAndGenerationStateMetrics> allIndexInformationsAndMetrics) {

  List<IndexInformation> indexInformations() {
    return this.allIndexInformationsAndMetrics.stream()
        .map(IndexInformationAndGenerationStateMetrics::indexInformation)
        .toList();
  }

  /**
   * For all present indexes, return the IndexGenerations and IndexConfigState grouped by IndexId.
   */
  Map<ObjectId, List<IndexGenerationStateMetrics>> allIndexGenerationsGrouped() {
    return this.allIndexInformationsAndMetrics.stream()
        .collect(
            CollectionUtils.toMapUniqueKeys(
                indexInfo -> indexInfo.indexInformation().getDefinition().getIndexId(),
                IndexInformationAndGenerationStateMetrics::indexGenerationStateMetrics));
  }

  record IndexInformationAndGenerationStateMetrics(
      IndexInformation indexInformation,
      List<IndexGenerationStateMetrics> indexGenerationStateMetrics) {}
}
