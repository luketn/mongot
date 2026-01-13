package com.xgen.mongot.util.mongodb;

import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.RequestContext;
import com.mongodb.client.ClientSession;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.client.internal.ClientSessionBinding;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A custom MongoDB client enabling efficient reads of paged results with a cursor by letting the
 * caller define on result type. The client supports any {@link ReadOperation} type which returns a
 * {@link BatchCursor}.
 *
 * <p>This client was created to enable search efficient reads, selecting a replica to read from as
 * specified by {@link com.mongodb.ReadPreference} in {@link MongoClientSettings}, and exposing
 * shard migrations with `showMigrationEvents` option on change-stream pipeline.
 */
public class BatchMongoClient implements AutoCloseable {

  private final Cluster cluster;
  private final MongoClientSettings settings;
  private final MongoClientImpl internalMongoClient;
  private final AtomicBoolean closed;

  public BatchMongoClient(MongoClientSettings settings) {
    Check.argNotNull(settings, "settings");
    MongoDriverInformation information = createMongoDriverInformation();
    Cluster dbCluster = createCluster(settings, information);

    this.settings = settings;
    this.cluster = dbCluster;
    this.internalMongoClient = createInternalMongoClient(dbCluster, information, settings);
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Creates a cursor for given read operation.
   *
   * @param readOperation A read operation to execute
   * @param session An open session. Must not be null.
   * @return A {@link BatchCursor} of given type
   */
  public <T> BatchCursor<T> openCursor(
      ReadOperation<BatchCursor<T>> readOperation, ClientSession session) {
    return readOperation.execute(createReadBinding(session));
  }

  /** Create a new client session with given options. */
  public ClientSession openSession(ClientSessionOptions options) {
    return this.internalMongoClient.startSession(options);
  }

  /** Returns the {@link MongoClientSettings} used by this client. */
  public MongoClientSettings getSettings() {
    return this.settings;
  }

  @Override
  public void close() {
    if (!this.closed.getAndSet(true)) {
      this.internalMongoClient.close();
      this.cluster.close();
    }
  }

  private MongoDriverInformation createMongoDriverInformation() {
    return MongoDriverInformation.builder().driverName("mongot-sync").build();
  }

  private Cluster createCluster(
      MongoClientSettings settings, MongoDriverInformation driverInformation) {

    return new DefaultClusterFactory()
        .createCluster(
            settings.getClusterSettings(),
            settings.getServerSettings(),
            settings.getConnectionPoolSettings(),
            InternalConnectionPoolSettings.builder().build(),
            getStreamFactory(settings, false),
            getStreamFactory(settings, true),
            settings.getCredential(),
            settings.getLoggerSettings(),
            getCommandListener(settings.getCommandListeners()),
            settings.getApplicationName(),
            driverInformation,
            settings.getCompressorList(),
            settings.getServerApi(),
            settings.getDnsClient(),
            settings.getInetAddressResolver());
  }

  private StreamFactory getStreamFactory(MongoClientSettings settings, boolean isHeartbeat) {
    SocketSettings socketSettings =
        isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();

    return Optional.ofNullable(settings.getStreamFactoryFactory())
        .map(factory -> factory.create(socketSettings, settings.getSslSettings()))
        .orElse(new SocketStreamFactory(socketSettings, settings.getSslSettings()));
  }

  private MongoClientImpl createInternalMongoClient(
      Cluster cluster, MongoDriverInformation driverInformation, MongoClientSettings settings) {

    return new MongoClientImpl(cluster, driverInformation, settings, null /* operationExecutor */);
  }

  private ReadBinding createReadBinding(ClientSession session) {
    Check.isNotNull(session, "session");

    Optional<SynchronousContextProvider> contextProvider =
        Optional.ofNullable(this.settings.getContextProvider())
            .map(
                provider -> {
                  Check.instanceOf(provider, SynchronousContextProvider.class);
                  return (SynchronousContextProvider) provider;
                });

    RequestContext requestContext =
        contextProvider
            .map(SynchronousContextProvider::getContext)
            .orElse(IgnorableRequestContext.INSTANCE);

    ClusterBinding clusterBinding =
        new ClusterBinding(
            this.cluster,
            this.settings.getReadPreference(),
            this.settings.getReadConcern(),
            this.settings.getServerApi(),
            requestContext);

    return new ClientSessionBinding(session, false /* ownsSession */, clusterBinding);
  }
}
