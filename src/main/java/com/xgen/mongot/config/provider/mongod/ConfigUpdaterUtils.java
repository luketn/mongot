package com.xgen.mongot.config.provider.mongod;

import static com.xgen.mongot.util.mongodb.Errors.INVALID_AUTHENTICATION_ERROR_CODES;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.MongoDbMetadataClient;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import io.micrometer.core.instrument.Tags;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class ConfigUpdaterUtils {

  private static final Tags callerTag = Tags.of("caller", ConfigUpdaterUtils.class.getName());

  private static List<IndexGeneration> getCollectionNotFoundIndexes(
      List<IndexGeneration> liveIndexGenerations) {
    return liveIndexGenerations.stream()
        .filter(indexGeneration -> indexGeneration.getIndex().getStatus().isCollectionNotFound())
        .toList();
  }

  /**
   * Get the set of collection UUIDs based on the current state of live indexes, resolving
   * collections directly from mongod where collections may have been missing previously.
   *
   * @param liveIndexGenerations A list of live index generations.
   * @param mongoDbMetadataClient A client for interacting with mongod to resolve collection info.
   * @param logger The logger instance to log any errors encountered during the process.
   * @return A set of collection UUIDs that are resolved directly from mongod, representing
   *     collections that are now accessible.
   */
  public static Set<UUID> resolveNonexistentUuidsOnDirectMongod(
      List<IndexGeneration> liveIndexGenerations,
      MongoDbMetadataClient mongoDbMetadataClient,
      Logger logger,
      Optional<MetricsFactory> metricsFactoryOpt) {

    // Get indexes with DOES_NOT_EXIST state due to collection not found
    List<IndexGeneration> collectionNotFoundIndexes =
        getCollectionNotFoundIndexes(liveIndexGenerations);
    // Group these indexes by database
    Map<String, List<IndexGeneration>> indexesByDatabase =
        collectionNotFoundIndexes.stream()
            .collect(
                Collectors.groupingBy(
                    indexGeneration -> indexGeneration.getDefinition().getDatabase()));

    Set<UUID> directMongodCollectionSet = new HashSet<>();
    for (Map.Entry<String, List<IndexGeneration>> entry : indexesByDatabase.entrySet()) {
      String database = entry.getKey();
      try {
        // Resolve collection info for the given database
        MongoDbCollectionInfos collectionInfos =
            mongoDbMetadataClient.resolveCollectionInfosOnDirectMongod(Set.of(database));

        // Add collection UUIDs from resolved collection info
        directMongodCollectionSet.addAll(collectionInfos.getAllCollectionUuids());
      } catch (CheckedMongoException e) {
        if (metricsFactoryOpt.isPresent()) {
          MetricsFactory metricsFactory = metricsFactoryOpt.get();
          metricsFactory.counter("failedConfCallGetCollectionInfos", callerTag).increment();

          if (e.getMongoException() != null
              && INVALID_AUTHENTICATION_ERROR_CODES.contains(e.getMongoException().getCode())) {
            metricsFactory.counter("authFailedConfCallGetCollectionInfos", callerTag).increment();
          }
        }

        logger.error(
            "Failed to retrieve collection information for database '{}', skipping. Error: {}",
            database,
            e.getMessage());
      }
    }
    return directMongodCollectionSet;
  }
}
