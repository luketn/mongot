package com.xgen.mongot.replication.mongodb.synonyms;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.uniqueMockGenerationDefinition;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_MAPPING_DEFINITION_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_MAPPING_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Keep;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.ControlledBlockingQueue;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class SynonymManagerTest {

  private static final BsonTimestamp SCAN_OPERATION_TIME = new BsonTimestamp(12345L);
  private static final SynonymMappingHighWaterMark SYNONYM_SCAN_HIGH_WATER_MARK =
      SynonymMappingHighWaterMark.create(SCAN_OPERATION_TIME);

  private static final SynonymDocumentIndexer DOCUMENT_INDEXER = mock(SynonymDocumentIndexer.class);

  @Test
  public void testSuccessfulSynonymSync() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_SYNONYM_MAPPING_ID, () -> SCAN_OPERATION_TIME));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable synonymSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<SynonymMappingHighWaterMark> future =
        mocks.synonymManager.enqueueCollectionScan(
            DOCUMENT_INDEXER,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            synonymSyncStartedCallback);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());
    mocks.mockScannerFactory.completeSync(MOCK_SYNONYM_MAPPING_ID);

    SynonymMappingHighWaterMark result = future.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(SYNONYM_SCAN_HIGH_WATER_MARK, result);
  }

  @Test
  public void testSynonymSyncExceptionSynonymSync() throws Exception {
    Mocks mocks =
        Mocks.create(
            Map.of(
                MOCK_SYNONYM_MAPPING_ID,
                () -> {
                  throw SynonymSyncException.createDropped();
                }));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable synonymSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<SynonymMappingHighWaterMark> future =
        mocks.synonymManager.enqueueCollectionScan(
            DOCUMENT_INDEXER,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            synonymSyncStartedCallback);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());
    mocks.mockScannerFactory.completeSync(MOCK_SYNONYM_MAPPING_ID);

    assertCompletesSynonymSyncException(future, SynonymSyncException.Type.DROPPED);
  }

  @Test
  public void testRuntimeExceptionSynonymSync() throws Exception {
    Mocks mocks =
        Mocks.create(
            Map.of(
                MOCK_SYNONYM_MAPPING_ID,
                () -> {
                  throw new RuntimeException();
                }));

    CountDownLatch startedLatch = new CountDownLatch(1);
    Runnable synonymSyncStartedCallback = startedLatch::countDown;

    CompletableFuture<SynonymMappingHighWaterMark> future =
        mocks.synonymManager.enqueueCollectionScan(
            DOCUMENT_INDEXER,
            mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            synonymSyncStartedCallback);

    startedLatch.await(5, TimeUnit.SECONDS);
    Assert.assertFalse(future.isDone());
    mocks.mockScannerFactory.completeSync(MOCK_SYNONYM_MAPPING_ID);

    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(RuntimeException.class);
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testConcurrentSyncs() throws Exception {
    List<SearchIndexDefinitionGeneration> definitions =
        IntStream.range(0, 4)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());
    List<SynonymMappingId> synonymMappingIds =
        definitions.stream()
            .map(
                def ->
                    SynonymMappingId.from(
                        def.getGenerationId(), MOCK_SYNONYM_MAPPING_DEFINITION_NAME))
            .collect(Collectors.toList());

    Mocks mocks =
        Mocks.create(
            Map.of(
                synonymMappingIds.get(0),
                () -> SCAN_OPERATION_TIME,
                synonymMappingIds.get(1),
                () -> SCAN_OPERATION_TIME,
                synonymMappingIds.get(2),
                () -> {
                  throw SynonymSyncException.createDropped();
                },
                synonymMappingIds.get(3),
                () -> {
                  throw SynonymSyncException.createDropped();
                }));

    List<CountDownLatch> startedLatches =
        IntStream.range(0, 4)
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toList());

    List<Runnable> startedCallbacks =
        startedLatches.stream()
            .<Runnable>map(latch -> latch::countDown)
            .collect(Collectors.toList());

    List<CompletableFuture<SynonymMappingHighWaterMark>> futures = new ArrayList<>();
    for (int i = 0; i != startedLatches.size(); i++) {
      futures.add(
          mocks.synonymManager.enqueueCollectionScan(
              DOCUMENT_INDEXER,
              mockIndexGeneration(definitions.get(i)),
              MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
              startedCallbacks.get(i)));
    }

    // 0, 1 should start.
    verifyScanStarted(0, mocks, startedLatches, synonymMappingIds);
    verifyScanStarted(1, mocks, startedLatches, synonymMappingIds);

    // 2, 3 should not start.
    verifyScanNotStarted(2, mocks, startedLatches, synonymMappingIds);
    verifyScanNotStarted(3, mocks, startedLatches, synonymMappingIds);

    // 2 should start after completing 0.
    completeAndGet(0, mocks, futures, synonymMappingIds);
    verifyScanStarted(2, mocks, startedLatches, synonymMappingIds);

    // 3 should remain not started.
    verifyScanNotStarted(3, mocks, startedLatches, synonymMappingIds);

    // 3 should start after completing 2
    completeAndGet(2, mocks, futures, synonymMappingIds);
    verifyScanStarted(3, mocks, startedLatches, synonymMappingIds);

    // 1, 3 finish
    completeAndGet(1, mocks, futures, synonymMappingIds);
    completeAndGet(3, mocks, futures, synonymMappingIds);
  }

  @Test
  public void testCancelQueued() throws Exception {
    List<SearchIndexDefinitionGeneration> definitions =
        IntStream.range(0, 3)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());
    List<SynonymMappingId> synonymMappingIds =
        definitions.stream()
            .map(
                def ->
                    SynonymMappingId.from(
                        def.getGenerationId(), MOCK_SYNONYM_MAPPING_DEFINITION_NAME))
            .collect(Collectors.toList());

    Mocks mocks =
        Mocks.create(
            Map.of(
                synonymMappingIds.get(0),
                () -> SCAN_OPERATION_TIME,
                synonymMappingIds.get(1),
                () -> SCAN_OPERATION_TIME,
                synonymMappingIds.get(2),
                () -> SCAN_OPERATION_TIME));

    List<CountDownLatch> startedLatches =
        IntStream.range(0, 3)
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toList());

    List<Runnable> startedCallbacks =
        startedLatches.stream()
            .<Runnable>map(latch -> latch::countDown)
            .collect(Collectors.toList());

    List<CompletableFuture<SynonymMappingHighWaterMark>> futures = new ArrayList<>();
    for (int i = 0; i != startedLatches.size(); i++) {
      futures.add(
          mocks.synonymManager.enqueueCollectionScan(
              DOCUMENT_INDEXER,
              mockIndexGeneration(definitions.get(i)),
              MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
              startedCallbacks.get(i)));
    }

    // 0, 1 should start.
    verifyScanStarted(0, mocks, startedLatches, synonymMappingIds);
    verifyScanStarted(1, mocks, startedLatches, synonymMappingIds);

    // 2 should not start.
    verifyScanNotStarted(2, mocks, startedLatches, synonymMappingIds);

    CompletableFuture<Void> cancelledFuture = mocks.synonymManager.cancel(synonymMappingIds.get(2));
    cancelledFuture.get(5, TimeUnit.SECONDS);
    assertCompletesSynonymSyncException(futures.get(2), SynonymSyncException.Type.SHUTDOWN);
  }

  @Test
  public void testSynonymSyncTwoMappingsDifferentIndexGeneration() throws Exception {
    SearchIndexDefinitionGeneration index0 = uniqueMockGenerationDefinition();
    SearchIndexDefinitionGeneration index1 =
        SearchIndexDefinitionGenerationBuilder.create(
            index0.getIndexDefinition().asSearchDefinition(),
            index0.generation().incrementUser(),
            Collections.emptyList());

    SynonymMappingId synonymMappingId0 =
        SynonymMappingId.from(index0.getGenerationId(), MOCK_SYNONYM_MAPPING_DEFINITION_NAME);
    SynonymMappingId synonymMappingId1 =
        SynonymMappingId.from(index1.getGenerationId(), MOCK_SYNONYM_MAPPING_DEFINITION_NAME);

    Mocks mocks =
        Mocks.create(
            Map.of(
                synonymMappingId0,
                () -> SCAN_OPERATION_TIME,
                synonymMappingId1,
                () -> SCAN_OPERATION_TIME));

    CountDownLatch startedLatch0 = new CountDownLatch(1);
    Runnable synonymSyncStartedCallback0 = startedLatch0::countDown;
    CompletableFuture<SynonymMappingHighWaterMark> future0 =
        mocks.synonymManager.enqueueCollectionScan(
            DOCUMENT_INDEXER,
            mockIndexGeneration(index0),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            synonymSyncStartedCallback0);

    CountDownLatch startedLatch1 = new CountDownLatch(1);
    Runnable synonymSyncStartedCallback1 = startedLatch1::countDown;
    CompletableFuture<SynonymMappingHighWaterMark> future1 =
        mocks.synonymManager.enqueueCollectionScan(
            DOCUMENT_INDEXER,
            mockIndexGeneration(index1),
            MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
            synonymSyncStartedCallback1);

    CompletableFuture<Void> cancelledFuture = mocks.synonymManager.cancel(synonymMappingId0);
    cancelledFuture.get(5, TimeUnit.SECONDS);

    assertCompletesSynonymSyncException(future0, SynonymSyncException.Type.SHUTDOWN);
    Assert.assertFalse(future1.isCompletedExceptionally());
    Assert.assertFalse(future1.isCancelled());

    mocks.mockScannerFactory.completeSync(synonymMappingId1);
    future1
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(throwable);
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testShutdownNothingInQueue() throws Exception {
    Mocks mocks = Mocks.create(Collections.emptyMap());
    CompletableFuture<Void> future = mocks.synonymManager.shutdown();
    Assert.assertTrue(future.isDone());
    future.get();
  }

  @Test
  public void testShutdownWithSyncs() throws Exception {
    List<SearchIndexDefinitionGeneration> definitions =
        IntStream.range(0, 4)
            .mapToObj(i -> uniqueMockGenerationDefinition())
            .collect(Collectors.toList());
    List<SynonymMappingId> synonymMappingIds =
        definitions.stream()
            .map(
                def ->
                    SynonymMappingId.from(
                        def.getGenerationId(), MOCK_SYNONYM_MAPPING_DEFINITION_NAME))
            .collect(Collectors.toList());

    Mocks mocks =
        Mocks.create(
            Map.of(
                synonymMappingIds.get(0),
                () -> SCAN_OPERATION_TIME,
                synonymMappingIds.get(1),
                () -> {
                  throw SynonymSyncException.createDropped();
                },
                synonymMappingIds.get(2),
                () -> SCAN_OPERATION_TIME,
                synonymMappingIds.get(3),
                () -> {
                  throw SynonymSyncException.createDropped();
                }));

    List<CountDownLatch> startedLatches =
        IntStream.range(0, 4)
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toList());

    List<Runnable> startedCallbacks =
        startedLatches.stream()
            .<Runnable>map(latch -> latch::countDown)
            .collect(Collectors.toList());

    List<CompletableFuture<SynonymMappingHighWaterMark>> futures = new ArrayList<>();
    for (int i = 0; i != startedLatches.size(); i++) {
      futures.add(
          mocks.synonymManager.enqueueCollectionScan(
              DOCUMENT_INDEXER,
              mockIndexGeneration(definitions.get(i)),
              MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
              startedCallbacks.get(i)));
    }

    // 0, 1 should start.
    verifyScanStarted(0, mocks, startedLatches, synonymMappingIds);
    verifyScanStarted(1, mocks, startedLatches, synonymMappingIds);

    // 2, 3 should not start.
    verifyScanNotStarted(2, mocks, startedLatches, synonymMappingIds);
    verifyScanNotStarted(3, mocks, startedLatches, synonymMappingIds);

    CompletableFuture<Void> shutdownFuture = mocks.synonymManager.shutdown();

    assertCompletesSynonymSyncException(futures.get(0), SynonymSyncException.Type.SHUTDOWN);
    assertCompletesSynonymSyncException(futures.get(1), SynonymSyncException.Type.SHUTDOWN);
    assertCompletesSynonymSyncException(futures.get(2), SynonymSyncException.Type.SHUTDOWN);
    assertCompletesSynonymSyncException(futures.get(3), SynonymSyncException.Type.SHUTDOWN);

    shutdownFuture.get();
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void testShutdownWhileCompletingSync() throws Exception {
    CountDownLatch synonymMappingShutdownInvoked = new CountDownLatch(1);
    CountDownLatch completeInvoked = new CountDownLatch(1);

    SynonymCollectionScanner scanner = mock(SynonymCollectionScanner.class);
    when(scanner.scan()).thenReturn(SCAN_OPERATION_TIME);

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsFactory metricsFactory = new MetricsFactory("synonymSync", meterRegistry);
    SynonymSyncMetrics metrics = new SynonymSyncMetrics(metricsFactory);

    SynonymManager realSynonymManager =
        SynonymManager.create(
            mock(SynonymSyncMongoClient.class),
            ControlledBlockingQueue.ready(),
            new HashMap<>(),
            new HashSet<>(),
            new HashMap<>(),
            2,
            (SynonymSyncMongoClient ignored1, SynonymCollectionScanRequest ignored2) -> scanner,
            metricsFactory);

    SynonymManager synonymManager = spy(realSynonymManager);

    // Answer for invocation of sync request future complete method.
    Answer<?> scanRequestFutureComplete =
        completeInvocation -> {
          // This thread completing a synonym sync holds the lock on SynonymManager. Wait for the
          // thread shutting down this synonym mapping manager to try and call cancel (e.g. wait for
          // the shutdown thread to hold the lock on the synonym mapping manager).
          completeInvoked.countDown();
          synonymMappingShutdownInvoked.await();
          return completeInvocation.callRealMethod();
        };

    // Answer for invocation of enqueueCollectionScan. Create a scan request in the same way the
    // real method would, except return a spy of the original completion future on getFuture()
    // calls. Spy on completion future allows us to intercept the complete call on that future and
    // answer with scanRequestFutureComplete.
    Answer<?> enqueueCollectionScan =
        enqueueScanInvocation -> {
          SynonymCollectionScanRequest realScanRequest =
              SynonymCollectionScanRequest.create(
                  enqueueScanInvocation.getArgument(0),
                  enqueueScanInvocation.getArgument(1),
                  enqueueScanInvocation.getArgument(2),
                  enqueueScanInvocation.getArgument(3),
                  (SynonymSyncMongoClient ignored1, SynonymCollectionScanRequest ignored2) ->
                      scanner,
                  metrics);

          SynonymCollectionScanRequest scanRequestSpy = spy(realScanRequest);

          var scanRequestFutureSpy = spy(scanRequestSpy.getFuture());

          doAnswer(scanRequestFutureComplete).when(scanRequestFutureSpy).complete(any());

          when(scanRequestSpy.getFuture()).thenReturn(scanRequestFutureSpy);

          // TODO(CLOUDP-280897): This isn't synchronized on the correct monitor. Either this test
          // needs to be restructured to not call this package-private method directly, or the class
          // needs to be restructured to (re)acquire its lock in this enqueue method.
          synchronized (synonymManager) {
            return synonymManager.enqueue(scanRequestSpy);
          }
        };

    doAnswer(enqueueCollectionScan)
        .when(synonymManager)
        .enqueueCollectionScan(any(), any(), any(), any());

    Answer<?> synonymManagerCancel =
        synonymManagerCancelInvocation -> {
          // Count down here to indicate the shutdown thread is in SynonymMappingManager::shutdown,
          // and has reached the point where it is about to call SynonymManager::cancel.
          // The thread counting down here now holds the lock on SynonymMappingManager.
          synonymMappingShutdownInvoked.countDown();
          // must defer call to non-spy synonym manager to be synchronized on same synonym
          // manager as scan request completion.
          return realSynonymManager.cancel(synonymManagerCancelInvocation.getArgument(0));
        };

    doAnswer(synonymManagerCancel).when(synonymManager).cancel(any());

    IndexGeneration indexGeneration = mockIndexGeneration();

    SynonymMappingDefinition synonymMappingDefinition =
        indexGeneration
            .getDefinition()
            .asSearchDefinition()
            .getSynonymMap()
            .get(MOCK_SYNONYM_MAPPING_DEFINITION_NAME);

    SynonymMappingManager synonymMappingManager =
        SynonymMappingManager.create(
            synonymManager,
            Executors.newCachedThreadPool(),
            (ignored0, ignored2) -> mock(SynonymDocumentIndexer.class),
            synonymMappingDefinition,
            indexGeneration,
            Duration.ZERO,
            Duration.ZERO);

    // Wait for scan to complete before shutting down synonym mapping. The thread invoking complete
    // holds a lock on the synonym manager when this latch counts down.
    completeInvoked.await();
    CompletableFuture<CompletableFuture<Void>> shutdownFuture =
        CompletableFuture.supplyAsync(
            synonymMappingManager::shutdown, Executors.newCachedThreadPool());

    shutdownFuture.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testCannotEnqueueAfterShutdown() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_SYNONYM_MAPPING_ID, () -> SCAN_OPERATION_TIME));
    mocks.synonymManager.shutdown();

    try {
      mocks.synonymManager.enqueueCollectionScan(
          DOCUMENT_INDEXER,
          mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
          MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
          () -> {
            // ignored
          });
    } catch (SynonymSyncException e) {
      Assert.assertTrue(e.isShutDown());
    }
  }

  @Test
  public void testCannotEnqueueExisting() throws Exception {
    Mocks mocks = Mocks.create(Map.of(MOCK_SYNONYM_MAPPING_ID, () -> SCAN_OPERATION_TIME));

    mocks.synonymManager.enqueueCollectionScan(
        DOCUMENT_INDEXER,
        mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
        MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
        () -> {
          // ignored
        });

    try {
      mocks.synonymManager.enqueueCollectionScan(
          DOCUMENT_INDEXER,
          mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
          MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
          () -> {
            // ignored
          });
      Assert.fail("should have thrown exception");
    } catch (IllegalStateException e) {
      // ignored
    }
  }

  @Test
  public void testCannotCancelNonexistent() throws Exception {
    Mocks mocks = Mocks.create(Collections.emptyMap());
    CompletableFuture<Void> cancelFuture = mocks.synonymManager.cancel(MOCK_SYNONYM_MAPPING_ID);
    cancelFuture.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testShutdownAfterTakenOffRequestQueue() throws Exception {
    ControlledBlockingQueue<SynonymMappingId> blockingQueue = ControlledBlockingQueue.paused();
    Mocks mocks =
        Mocks.create(Map.of(MOCK_SYNONYM_MAPPING_ID, () -> SCAN_OPERATION_TIME), blockingQueue);

    mocks.synonymManager.enqueueCollectionScan(
        DOCUMENT_INDEXER,
        mockIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION),
        MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION,
        () -> {});
    CompletableFuture<Void> shutdownFuture =
        CompletableFuture.runAsync(mocks.synonymManager::shutdown);
    Thread.sleep(500); // Wait for shutdown() to interrupt the dispatcher thread
    blockingQueue.resume();

    shutdownFuture.get(5, TimeUnit.SECONDS);
  }

  private void verifyScanStarted(
      int idx,
      Mocks mocks,
      List<CountDownLatch> startedLatches,
      List<SynonymMappingId> synonymMappingIds)
      throws Exception {
    verifyScanStarted(mocks, startedLatches.get(idx), synonymMappingIds.get(idx));
  }

  private void verifyScanStarted(
      Mocks mocks, CountDownLatch startedLatch, SynonymMappingId synonymMappingId)
      throws Exception {
    startedLatch.await(5, TimeUnit.SECONDS);
    verify(mocks.mockScannerFactory.getScanner(synonymMappingId), timeout(1000)).scan();
  }

  private void verifyScanNotStarted(
      int idx,
      Mocks mocks,
      List<CountDownLatch> startedLatches,
      List<SynonymMappingId> synonymMappingIds)
      throws Exception {
    verifyScanNotStarted(mocks, startedLatches.get(idx), synonymMappingIds.get(idx));
  }

  private void verifyScanNotStarted(
      Mocks mocks, CountDownLatch startedLatch, SynonymMappingId synonymMappingId)
      throws Exception {
    Assert.assertEquals(1, startedLatch.getCount());
    verify(mocks.mockScannerFactory.getScanner(synonymMappingId), times(0)).scan();
  }

  private void completeAndGet(
      int idx,
      Mocks mocks,
      List<CompletableFuture<SynonymMappingHighWaterMark>> futures,
      List<SynonymMappingId> synonymMappingIds)
      throws Exception {
    completeAndGet(mocks, futures.get(idx), synonymMappingIds.get(idx));
  }

  private void completeAndGet(
      Mocks mocks,
      CompletableFuture<SynonymMappingHighWaterMark> future,
      SynonymMappingId synonymMappingId)
      throws Exception {
    mocks.mockScannerFactory.completeSync(synonymMappingId);
    FutureUtils.swallowedFuture(future).get(5, TimeUnit.SECONDS);
  }

  private static void assertCompletesSynonymSyncException(
      CompletableFuture<?> future, SynonymSyncException.Type type) throws Exception {
    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(SynonymSyncException.class);
              Assert.assertEquals(type, ((SynonymSyncException) throwable).getType());
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  private static class Mocks {
    @Keep final SynonymSyncMongoClient mongoClient;
    @Keep final BlockingQueue<SynonymMappingId> requestQueue;
    @Keep final Map<SynonymMappingId, SynonymSyncRequest> queued;
    @Keep final Set<SynonymMappingId> cancelled;
    @Keep final Map<SynonymMappingId, InProgressSynonymSyncInfo> syncInProgress;

    final MockSynonymCollectionScannerFactory mockScannerFactory;
    final SynonymManager synonymManager;

    private Mocks(
        SynonymSyncMongoClient mongoClient,
        BlockingQueue<SynonymMappingId> requestQueue,
        Map<SynonymMappingId, SynonymSyncRequest> queued,
        Set<SynonymMappingId> cancelled,
        Map<SynonymMappingId, InProgressSynonymSyncInfo> syncInProgress,
        int numConcurrentSyncs,
        MockSynonymCollectionScannerFactory mockScannerFactory) {
      this.mongoClient = mongoClient;
      this.requestQueue = requestQueue;
      this.queued = queued;
      this.cancelled = cancelled;
      this.syncInProgress = syncInProgress;
      this.mockScannerFactory = mockScannerFactory;

      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      MetricsFactory metricsFactory = new MetricsFactory("synonymSync", meterRegistry);

      this.synonymManager =
          SynonymManager.create(
              mongoClient,
              requestQueue,
              queued,
              cancelled,
              syncInProgress,
              numConcurrentSyncs,
              mockScannerFactory,
              metricsFactory);
    }

    static Mocks create(
        Map<SynonymMappingId, CheckedSupplier<BsonTimestamp, SynonymSyncException>> resultSuppliers,
        BlockingQueue<SynonymMappingId> blockingQueue)
        throws Exception {

      return new Mocks(
          mock(SynonymSyncMongoClient.class),
          blockingQueue,
          new HashMap<>(),
          new HashSet<>(),
          new HashMap<>(),
          2,
          new MockSynonymCollectionScannerFactory(resultSuppliers));
    }

    static Mocks create(
        Map<SynonymMappingId, CheckedSupplier<BsonTimestamp, SynonymSyncException>> resultSuppliers)
        throws Exception {

      return Mocks.create(resultSuppliers, ControlledBlockingQueue.ready());
    }

    private static class MockSynonymCollectionScannerFactory
        implements SynonymCollectionScanner.Factory {

      private final Map<SynonymMappingId, CheckedSupplier<BsonTimestamp, SynonymSyncException>>
          resultSuppliers;
      private final Map<SynonymMappingId, CountDownLatch> latches;
      private final Map<SynonymMappingId, SynonymCollectionScanner> scanners;

      private MockSynonymCollectionScannerFactory(
          Map<SynonymMappingId, CheckedSupplier<BsonTimestamp, SynonymSyncException>>
              resultSuppliers) {
        this.resultSuppliers = resultSuppliers;

        this.latches =
            resultSuppliers.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new CountDownLatch(1)));

        this.scanners =
            resultSuppliers.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                          SynonymCollectionScanner scanner = mock(SynonymCollectionScanner.class);
                          CountDownLatch latch = this.latches.get(entry.getKey());
                          CheckedSupplier<BsonTimestamp, SynonymSyncException> resultSupplier =
                              this.resultSuppliers.get(entry.getKey());

                          try {
                            when(scanner.scan())
                                .then(
                                    ignored -> {
                                      try {
                                        latch.await();
                                      } catch (InterruptedException e) {
                                        throw SynonymSyncException.createShutDown();
                                      }
                                      return resultSupplier.get();
                                    });
                          } catch (SynonymSyncException e) {
                            throw new RuntimeException(e);
                          }

                          return scanner;
                        }));
      }

      @Override
      public SynonymCollectionScanner create(
          SynonymSyncMongoClient mongoClient, SynonymCollectionScanRequest syncRequest) {
        return this.scanners.get(syncRequest.getMappingId());
      }

      void completeSync(SynonymMappingId mappingId) {
        this.latches.get(mappingId).countDown();
      }

      SynonymCollectionScanner getScanner(SynonymMappingId mappingId) {
        return this.scanners.get(mappingId);
      }
    }
  }
}
