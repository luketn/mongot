package com.xgen.mongot.server.command.management.aic;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.IndexEntry;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.util.CollectionUtils;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.bson.BsonDocument;

/**
 * This alternate implementation of the ListSearchIndexesCommand is used during internal e2e
 * testing, primarily as a workaround to limitations of the index management commands when compared
 * to the custom testing infrastructure (mock mms) available in the Atlas environment.
 *
 * <p>The primary changes to behavior are extended index status information, and returning all known
 * search indexes when called with the internal catalog collection name.
 */
public final class AicExtendedListSearchIndexesCommand extends AicListSearchIndexesCommand {

  // These should be kept consistent with the names defined in AuthoritativeIndexCatalogMongoClient.
  private static final String CATALOG_DATABASE_NAME = "__mdb_internal_search";
  private static final String CATALOG_COLLECTION_NAME = "indexCatalog";

  private final CachedIndexInfoProvider indexInfoProvider;
  private final boolean listAllIndexes;

  AicExtendedListSearchIndexesCommand(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      CachedIndexInfoProvider indexInfoProvider,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      ListSearchIndexesCommandDefinition definition) {
    super(authoritativeIndexCatalog, db, collectionUuid, collectionName, view, definition);
    this.indexInfoProvider = indexInfoProvider;

    this.listAllIndexes =
        db.equals(CATALOG_DATABASE_NAME) && collectionName.equals(CATALOG_COLLECTION_NAME);
  }

  @Override
  Stream<IndexDefinition> findMatchingIndexes() {
    if (this.listAllIndexes) {
      return getAuthoritativeIndexCatalog().listIndexes().stream();
    }
    return super.findMatchingIndexes();
  }

  @Override
  List<BsonDocument> populateResponseData(Stream<IndexDefinition> matchingIndexes) {
    var indexInformationById =
        this.indexInfoProvider.getIndexInfos().stream()
            .collect(
                CollectionUtils.toMapUniqueKeys(
                    ii -> ii.getDefinition().getIndexId(), Function.identity()));
    return matchingIndexes
        .map(
            indexDefinition -> {
              if (!indexInformationById.containsKey(indexDefinition.getIndexId())) {
                return IndexEntry.fromIndexDefinition(indexDefinition, StatusCode.UNKNOWN, 0L);
              }
              var indexInformation = indexInformationById.get(indexDefinition.getIndexId());
              return IndexEntry.fromIndexDefinition(
                  indexDefinition,
                  indexInformation.getStatus().getStatusCode(),
                  indexInformation.getAggregatedMetrics().numDocs());
            })
        .map(IndexEntry::toBson)
        .toList();
  }
}
