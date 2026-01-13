package com.xgen.mongot.embedding.mongodb.leasing;

import static com.xgen.mongot.embedding.mongodb.leasing.StatusResolutionUtils.getEffectiveMaterializedViewStatus;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lease manager that uses a static leader. Leadership status is pre-assigned and remains constant
 * for the lifetime of the process. Uses a MongoDB collection to store leases.
 *
 * <p>Expected to be used as a singleton.
 */
public class StaticLeaderLeaseManager implements LeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(StaticLeaderLeaseManager.class);

  private static final String AUTO_EMBEDDING_INTERNAL_DATABASE_NAME = "__mdb_internal_search";

  private static final String METRICS_NAMESPACE = "embedding.leasing.stats";

  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

  // placeholder value until we see the need to change this.
  private static final int MONGO_CLIENT_MAX_CONNECTIONS = 2;

  @VisibleForTesting static final String LEASE_COLLECTION_NAME = "auto_embedding_leases";

  public static final long DEFAULT_INDEX_DEFINITION_VERSION = 0;

  private final MongoClientOperationExecutor operationExecutor;
  private final String hostname;
  // Mapping of leases to index Ids.
  private final Map<String, Lease> leases;
  private final MongoCollection<Lease> collection;
  private final boolean isLeader;

  public StaticLeaderLeaseManager(
      MongoClient mongoClient,
      MetricsFactory metricsFactory,
      String hostname,
      String databaseName,
      boolean isLeader) {
    this.operationExecutor =
        new MongoClientOperationExecutor(metricsFactory, "leaseTableCollection");
    this.hostname = hostname;
    this.leases = new ConcurrentHashMap<>();
    // Use LINEARIZABLE read concern for lease operations to ensure we always read the most
    // up-to-date lease state. This is critical for lease correctness.
    // LINEARIZABLE read concern requires ReadPreference.primary() to work correctly.
    this.collection =
        mongoClient
            .getDatabase(databaseName)
            .getCollection(LEASE_COLLECTION_NAME, Lease.class)
            .withReadConcern(ReadConcern.LINEARIZABLE)
            .withReadPreference(ReadPreference.primary());
    this.isLeader = isLeader;
    if (this.isLeader) {
      initLeases();
    }
  }

  public static StaticLeaderLeaseManager create(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      String hostname,
      boolean isLeader) {
    return new StaticLeaderLeaseManager(
        getMongoClient(syncSourceConfig, meterAndFtdcRegistry),
        new MetricsFactory(METRICS_NAMESPACE, meterAndFtdcRegistry.meterRegistry()),
        hostname,
        AUTO_EMBEDDING_INTERNAL_DATABASE_NAME,
        isLeader);
  }

  /**
   * Initializes the local lease state with the leases from the database.
   *
   * @throws Exception if there is an error talking to the database.
   */
  private void initLeases() {
    try {
      List<Lease> leases =
          this.operationExecutor.execute(
              "getLeases", () -> this.collection.find().into(new ArrayList<>()));
      leases.forEach(lease -> this.leases.put(lease.id(), lease));
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize leases from database.", e);
    }
  }

  @Override
  public void add(IndexGeneration indexGeneration) {
    // Create the lease if it doesn't exist. The lease can already exist in the case of process
    // restarts.
    // If the lease already exists, then just add the index generation to the lease.
    // Note that we only add the lease in memory here, we write to the database only when we update
    // the commit info.
    if (this.leases.containsKey(getLeaseKey(indexGeneration.getGenerationId()))) {
      var lease = this.leases.get(getLeaseKey(indexGeneration.getGenerationId()));
      String versionKey =
          String.valueOf(
              indexGeneration
                  .getDefinition()
                  .getDefinitionVersion()
                  .orElse(DEFAULT_INDEX_DEFINITION_VERSION));
      if (!lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
        this.leases.put(
            getLeaseKey(indexGeneration.getGenerationId()),
            lease.withNewIndexDefinitionVersion(
                getIndexDefinitionVersion(indexGeneration),
                indexGeneration.getIndex().getStatus()));
      }
    } else {
      Lease lease =
          Lease.newLease(
              indexGeneration.getDefinition().getIndexId().toHexString(),
              indexGeneration.getDefinition().getCollectionUuid(),
              indexGeneration.getDefinition().getLastObservedCollectionName(),
              this.hostname,
              getIndexDefinitionVersion(indexGeneration),
              indexGeneration.getIndex().getStatus());
      this.leases.put(getLeaseKey(indexGeneration.getGenerationId()), lease);
    }
  }

  @Override
  public CompletableFuture<Void> drop(GenerationId generationId) {
    // The current drop implementation only handles index/lease deletion and not index generation
    // deletion. This is because there might be followers that are still relying on the status of
    // this index generation. We could potentially put an upper bound on the number of index
    // generations we track to prevent the status map from growing unbounded.
    // Note that we're relying on MaterializedViewManager to do the right thing based on reference
    // counting and only invoke this method when the last index generation is being dropped.
    if (this.isLeader) {
      return CompletableFuture.runAsync(
          () -> {
            this.collection.deleteOne(new Document("_id", getLeaseKey(generationId)));
            this.leases.remove(getLeaseKey(generationId));
          });
    } else {
      this.leases.remove(getLeaseKey(generationId));
      return COMPLETED_FUTURE;
    }
  }

  @Override
  public boolean isLeader(GenerationId generationId) {
    return this.isLeader;
  }

  @Override
  public void updateCommitInfo(GenerationId generationId, EncodedUserData encodedUserData) {
    ensureLeaseExists(generationId);
    ensureLeader();
    Lease currentLease = this.leases.get(getLeaseKey(generationId));
    Lease updatedLease = currentLease.withUpdatedCheckpoint(encodedUserData);
    updateLeaseInDatabase(generationId, currentLease, updatedLease, encodedUserData);
  }

  @Override
  public EncodedUserData getCommitInfo(GenerationId generationId) throws IOException {
    // Leader can read from in-memory state.
    if (this.isLeader) {
      ensureLeaseExists(generationId);
      return EncodedUserData.fromString(this.leases.get(getLeaseKey(generationId)).commitInfo());
    }
    // If follower, read from database. Although technically, a follower should never call this
    // method as we use a no-op replication manager on followers.
    try {
      Lease lease = getLeaseFromDatabase(generationId);
      if (lease == null) {
        return EncodedUserData.EMPTY;
      }
      return EncodedUserData.fromString(lease.commitInfo());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateReplicationStatus(
      GenerationId generationId, long indexDefinitionVersion, IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    // only update status in database if leader. Followers may still call this method, but we treat
    // it as a no-op.
    if (this.isLeader) {
      ensureLeaseExists(generationId);
      Lease currentLease = this.leases.get(getLeaseKey(generationId));
      Lease updatedLease = currentLease.withUpdatedStatus(indexStatus, indexDefinitionVersion);
      updateLeaseInDatabase(
          generationId,
          currentLease,
          updatedLease,
          EncodedUserData.fromString(currentLease.commitInfo()));
    }
  }

  @Override
  public IndexStatus getMaterializedViewReplicationStatus(IndexGeneration indexGeneration) {
    // Leader reads from in-memory state.
    @Var
    Lease.IndexDefinitionVersionStatus requestedIndexDefinitionVersionStatus =
        new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN);
    @Var
    Lease.IndexDefinitionVersionStatus latestIndexDefinitionVersionStatus =
        new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.UNKNOWN);
    if (this.isLeader) {
      if (this.leases.containsKey(getLeaseKey(indexGeneration.getGenerationId()))) {
        var lease = this.leases.get(getLeaseKey(indexGeneration.getGenerationId()));
        requestedIndexDefinitionVersionStatus =
            lease
                .indexDefinitionVersionStatusMap()
                .get(String.valueOf(indexGeneration.getDefinition().getDefinitionVersion()));
        latestIndexDefinitionVersionStatus =
            lease.indexDefinitionVersionStatusMap().get(lease.latestIndexDefinitionVersion());
      }
    } else {
      // Follower reads from database.
      try {
        Lease lease = getLeaseFromDatabase(indexGeneration.getGenerationId());
        if (lease != null) {
          String versionKey =
              String.valueOf(
                  indexGeneration
                      .getDefinition()
                      .getDefinitionVersion()
                      .orElse(DEFAULT_INDEX_DEFINITION_VERSION));
          if (versionKey != null
              && lease.indexDefinitionVersionStatusMap().containsKey(versionKey)) {
            requestedIndexDefinitionVersionStatus =
                lease.indexDefinitionVersionStatusMap().get(versionKey);
          } else {
            LOG.warn(
                "Requested version key {} not found in lease for generation ID {}",
                versionKey,
                indexGeneration);
          }
          // we will always have an entry corresponding to the latest index definition version.
          latestIndexDefinitionVersionStatus =
              lease.indexDefinitionVersionStatusMap().get(lease.latestIndexDefinitionVersion());
        }
      } catch (Exception e) {
        LOG.warn(
            "Failed to get index status for {}", getLeaseKey(indexGeneration.getGenerationId()), e);
      }
    }
    return getEffectiveMaterializedViewStatus(
        requestedIndexDefinitionVersionStatus, latestIndexDefinitionVersionStatus);
  }

  @VisibleForTesting
  Map<String, Lease> getLeases() {
    return this.leases;
  }

  private void ensureLeaseExists(GenerationId generationId) {
    if (!this.leases.containsKey(getLeaseKey(generationId))) {
      throw new IllegalStateException("Lease does not exist for " + getLeaseKey(generationId));
    }
  }

  private void ensureLeader() {
    if (!this.isLeader) {
      throw new IllegalStateException(
          "Attempting to update lease state while not being the leader");
    }
  }

  @VisibleForTesting
  // Generates the lease key/ID to use for the given generation ID. For now, we derive this using
  // the same logic that we use to derive the mat view collection name - which is the index ID.
  // This means that we will have a single lease document per index across all its generations.
  static String getLeaseKey(GenerationId generationId) {
    return generationId.indexId.toHexString();
  }

  private Lease getLeaseFromDatabase(GenerationId generationId) throws Exception {
    return this.operationExecutor.execute(
        "getLease",
        () -> this.collection.find(new Document("_id", getLeaseKey(generationId))).first());
  }

  private void updateLeaseInDatabase(
      GenerationId generationId,
      Lease currentLease,
      Lease updatedLease,
      EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException {
    try {
      if (currentLease.leaseVersion() == Lease.FIRST_LEASE_VERSION) {
        // Lease document doesn't exist yet, so we can use a simple upsert.
        var filter = Filters.eq("_id", getLeaseKey(generationId));
        this.operationExecutor.execute(
            "createLease", () -> this.collection.replaceOne(filter, updatedLease, REPLACE_OPTIONS));
        this.leases.put(getLeaseKey(generationId), updatedLease);
      } else {
        // Update with optimistic concurrency control on the lease version to ensure the lease
        // doesn't get updated with a stale checkpoint.
        // The filter first finds the document with the correct ID. Then it ensures the document
        // is in one of two states:
        // 1. The document has the same lease version as the local lease. This means the document
        //    has not been updated and the update can proceed.
        // 2. The document has been updated by a previous call which was processed by the server but
        // not
        //    acknowledged by the client. In this case, we check both the version and the commit
        // info to
        //    ensure it's in the desired state already. An update here is thus a no-op and safe.
        var filter =
            Filters.and(
                Filters.eq("_id", getLeaseKey(generationId)),
                Filters.or(
                    Filters.eq(Lease.Fields.LEASE_VERSION.getName(), currentLease.leaseVersion()),
                    Filters.and(
                        Filters.eq(
                            Lease.Fields.LEASE_VERSION.getName(), updatedLease.leaseVersion()),
                        // we could potentially do a deeper check across more fields here for more
                        // safety.
                        Filters.eq(
                            Lease.Fields.COMMIT_INFO.getName(), encodedUserData.asString()))));
        var result =
            this.operationExecutor.execute(
                "updateLease", () -> this.collection.replaceOne(filter, updatedLease));
        if (result.getMatchedCount() == 0) {
          // This means the document was not in one of the two desired states described above.
          // TODO(CLOUDP-364787): We should move the index to failed state as this is not a
          // recoverable error.
          LOG.warn(
              "Failed to update lease for {} due to version mismatch. Local lease is {}. ",
              getLeaseKey(generationId),
              updatedLease);
          throw new MaterializedViewNonTransientException(
              "Fails to update lease for "
                  + getLeaseKey(generationId)
                  + "please check Lease collection to clean up corrupted records");
        } else {
          this.leases.put(getLeaseKey(generationId), updatedLease);
        }
      }
    } catch (Exception e) {
      // Only this lease has problem, fails this index only but keeps Mongot alive.
      if (e instanceof MaterializedViewNonTransientException matViewNonTransientException) {
        throw matViewNonTransientException;
      }
      // TODO(CLOUDP-371153): we need to handle this appropriately in MatViewGenerator as updating
      // status can fail
      throw new MaterializedViewTransientException(e);
    }
  }

  private static MongoClient getMongoClient(
      SyncSourceConfig syncSourceConfig, MeterAndFtdcRegistry meterAndFtdcRegistry) {
    // Use mongosUri if available, otherwise fall back to mongodUri. This allows the MongoDB driver
    // to automatically discover replica set topology and route writes to the primary, avoiding
    // NotWritablePrimary errors after failovers.
    var connectionString = syncSourceConfig.mongosUri.orElse(syncSourceConfig.mongodUri);
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        connectionString,
        "Lease Manager mongo client",
        MONGO_CLIENT_MAX_CONNECTIONS,
        syncSourceConfig.sslContext,
        meterAndFtdcRegistry.meterRegistry());
  }

  private String getIndexDefinitionVersion(IndexGeneration indexGeneration) {
    return String.valueOf(
        indexGeneration
            .getDefinition()
            .getDefinitionVersion()
            .orElse(DEFAULT_INDEX_DEFINITION_VERSION));
  }
}
