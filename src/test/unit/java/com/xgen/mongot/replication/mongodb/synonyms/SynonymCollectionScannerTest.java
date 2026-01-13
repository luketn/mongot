package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DATABASE_NAME;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_MAPPING_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_SYNONYM_SOURCE_COLLECTION_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.replication.mongodb.common.CollectionScanCommandMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class SynonymCollectionScannerTest {
  private static final BsonTimestamp MIN_VALID_OPTIME = new BsonTimestamp(12345);
  private static final BsonTimestamp START_OPTIME = new BsonTimestamp(1234);

  @Test
  public void testNoDocuments() throws Exception {
    Mocks mocks = Mocks.noDocuments();

    BsonTimestamp opTime = mocks.collectionScanner.scan();
    verify(mocks.documentIndexer, times(0)).indexDocumentBatch(any());
    verify(mocks.collectionScanner, never()).cancelProcessing(any());
    Assert.assertSame(START_OPTIME, opTime);
  }

  @Test
  public void testScanSuccessCompletes() throws Exception {
    Mocks mocks = Mocks.threeDocuments();

    BsonTimestamp opTime = mocks.collectionScanner.scan();
    verify(mocks.documentIndexer, times(1)).indexDocumentBatch(argThat(batch -> batch.size() == 3));
    verify(mocks.collectionScanner, never()).cancelProcessing(any());
    Assert.assertSame(START_OPTIME, opTime);
  }

  @Test
  public void testShutdown() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    // Start scanning documents in a separate thread.
    AtomicReference<SynonymSyncException> exceptionReference = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                mocks.collectionScanner.scan();
              } catch (SynonymSyncException e) {
                exceptionReference.set(e);
              }
            });

    mocks.collectionScanner.signalShutdown();

    scannerThread.start();

    verify(mocks.documentIndexer, never()).indexDocumentBatch(any());
    scannerThread.join(2500);

    verify(mocks.collectionScanner).handleShutdown();

    Assert.assertNotNull("should throw exception", exceptionReference.get());
    Assert.assertEquals(SynonymSyncException.Type.SHUTDOWN, exceptionReference.get().getType());
  }

  @Test
  public void testShutdownRightBeforeIndexingError() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    CountDownLatch indexingStarted = new CountDownLatch(1);
    CountDownLatch shutdownSignalled = new CountDownLatch(1);
    doAnswer(
            (unused) -> {
              indexingStarted.countDown();
              shutdownSignalled.await();
              throw new IllegalArgumentException();
            })
        .when(mocks.documentIndexer)
        .indexDocumentBatch(any());

    // Start scanning documents in a separate thread.
    AtomicReference<IllegalArgumentException> exceptionReference = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                mocks.collectionScanner.scan();
              } catch (SynonymSyncException e) {
                // ignored - we expect IllegalArgumentException in this test
              } catch (IllegalArgumentException e) {
                exceptionReference.set(e);
              }
            });
    scannerThread.start();

    indexingStarted.await();

    mocks.collectionScanner.signalShutdown();

    shutdownSignalled.countDown();
    scannerThread.join(2500);

    // verify cancel was called on the scheduler
    verify(mocks.collectionScanner, atLeastOnce()).cancelProcessing(any());

    Assert.assertNotNull(
        "CollectionScanner did not throw IllegalArgumentException", exceptionReference.get());
  }

  @Test
  public void testScanIsInterruptable() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();

    // Start scanning documents in a separate thread.
    AtomicReference<SynonymSyncException> exceptionReference = new AtomicReference<>();
    Thread scannerThread =
        new Thread(
            () -> {
              try {
                mocks.collectionScanner.scan();
              } catch (SynonymSyncException e) {
                exceptionReference.set(e);
              }
            });
    scannerThread.start();

    // Wait until we've started the scan, then shut down the scanner.
    verify(mocks.documentIndexer, timeout(2500).atLeastOnce()).indexDocumentBatch(any());

    mocks.collectionScanner.signalShutdown();
    scannerThread.join(2500);

    // verify shutdown was called on the scanner
    verify(mocks.collectionScanner).handleShutdown();

    Assert.assertNotNull("should throw", exceptionReference.get());
    Assert.assertEquals(SynonymSyncException.Type.SHUTDOWN, exceptionReference.get().getType());
  }

  @Test
  public void testScanWrapsInvalidSynonymDocument() throws Exception {
    Exception exception;
    try {
      SynonymSyncException.wrapIfThrows(
          () -> {
            throw SynonymMappingException.invalidSynonymDocument(
                new BsonParseException("invalid synonym document", Optional.empty()));
          });

      throw new AssertionError("should throw above");
    } catch (SynonymSyncException e) {
      exception = e;
    }

    Mocks mocks = Mocks.documentIndexerFailsOnInvalidSynonymDocument(exception);
    var e = Assert.assertThrows(SynonymSyncException.class, mocks.collectionScanner::scan);

    verify(mocks.collectionScanner).cancelProcessing(eq(e));

    Assert.assertEquals(SynonymSyncException.Type.INVALID, e.getType());
  }

  @Test
  public void testScanWrapsMongoException() throws Exception {
    Exception exception;
    try {
      SynonymSyncException.wrapIfThrows(
          () -> {
            throw new MongoException("mongo exception");
          });

      throw new AssertionError("should throw above");
    } catch (SynonymSyncException e) {
      exception = e;
    }

    Mocks mocks = Mocks.mongoClientFails(exception);

    try {
      mocks.collectionScanner.scan();
    } catch (SynonymSyncException scanException) {
      verify(mocks.documentIndexer, never()).indexDocumentBatch(any());
      verify(mocks.collectionScanner).cancelProcessing(eq(exception));
      Assert.assertSame(exception, scanException);

      Assert.assertEquals(SynonymSyncException.Type.TRANSIENT, scanException.getType());
      return;
    }

    Assert.fail("should have thrown exception");
  }

  @Test
  public void testScanWrapsExceeded() throws Exception {
    Mocks mocks = Mocks.infiniteDocuments();
    doThrow(
            SynonymSyncException.createFieldExceeded(
                new FieldExceededLimitsException("too many fields")))
        .when(mocks.documentIndexer)
        .indexDocumentBatch(any());

    var e = Assert.assertThrows(SynonymSyncException.class, mocks.collectionScanner::scan);

    verify(mocks.collectionScanner).cancelProcessing(eq(e));
    Assert.assertEquals(SynonymSyncException.Type.FIELD_EXCEEDED, e.getType());
  }

  private static class Mocks {
    private final SynonymSyncMongoClient mongoClient;
    private final SynonymCollectionScanRequest syncRequest;
    private final CollectionScanMongoClient<SynonymSyncException> findCommandMongoClient;

    private final SynonymDocumentIndexer documentIndexer;

    private final SynonymCollectionScanner collectionScanner;

    private Mocks(
        SynonymSyncMongoClient mongoClient,
        SynonymCollectionScanRequest syncRequest,
        CollectionScanMongoClient<SynonymSyncException> findCommandMongoClient,
        SynonymDocumentIndexer documentIndexer) {
      this.mongoClient = mongoClient;
      this.syncRequest = syncRequest;
      this.findCommandMongoClient = findCommandMongoClient;

      this.documentIndexer = documentIndexer;

      this.collectionScanner =
          spy(new SynonymCollectionScanner(this.mongoClient, this.syncRequest));
    }

    private static Mocks createMocks() throws Exception {
      return createMocks(Optional.of(START_OPTIME));
    }

    private static Mocks createMocks(Optional<BsonTimestamp> opTime) throws Exception {
      @SuppressWarnings("unchecked")
      CollectionScanMongoClient<SynonymSyncException> findCommandMongoClient =
          (CollectionScanMongoClient<SynonymSyncException>)
              mock(CollectionScanCommandMongoClient.class);
      when(findCommandMongoClient.getMinValidOpTime()).thenReturn(MIN_VALID_OPTIME);

      when(findCommandMongoClient.getOperationTime()).thenReturn(opTime.get());

      SynonymSyncMongoClient mongoClient = mock(SynonymSyncMongoClient.class);
      when(mongoClient.getFindCommandClient(any(), any())).thenReturn(findCommandMongoClient);

      CompletableFuture<SynonymMappingHighWaterMark> future = new CompletableFuture<>();
      MongoNamespace namespace =
          new MongoNamespace(MOCK_INDEX_DATABASE_NAME, MOCK_SYNONYM_SOURCE_COLLECTION_NAME);
      SynonymDocumentIndexer documentIndexer = mock(SynonymDocumentIndexer.class);

      SynonymCollectionScanRequest syncRequest = mock(SynonymCollectionScanRequest.class);
      when(syncRequest.getSynonymMappingDefinition())
          .thenReturn(MOCK_SINGLE_SYNONYM_MAPPING_DEFINITION);
      when(syncRequest.getNamespace()).thenReturn(namespace);
      when(syncRequest.getDocumentIndexer()).thenReturn(documentIndexer);
      when(syncRequest.getFuture()).thenReturn(future);
      when(syncRequest.getMappingId()).thenReturn(MOCK_SYNONYM_MAPPING_ID);

      return new Mocks(mongoClient, syncRequest, findCommandMongoClient, documentIndexer);
    }

    static Mocks noDocuments() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(false);
      when(mocks.findCommandMongoClient.getNext()).thenReturn(List.of());

      return mocks;
    }

    static Mocks threeDocuments() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true).thenReturn(false);
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(
              List.of(
                  BsonUtils.documentToRaw(new BsonDocument("_id", new BsonInt32(1))),
                  BsonUtils.documentToRaw(new BsonDocument("_id", new BsonInt32(3))),
                  BsonUtils.documentToRaw(new BsonDocument("_id", new BsonInt32(5)))));
      return mocks;
    }

    static Mocks infiniteDocuments() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true);
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(List.of(BsonUtils.documentToRaw(new BsonDocument())));
      return mocks;
    }

    @SuppressWarnings("unused")
    static Mocks documentThenThrowTransient() throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true);
      when(mocks.findCommandMongoClient.getNext())
          .thenReturn(List.of(BsonUtils.documentToRaw(new BsonDocument())))
          .thenThrow(SynonymSyncException.createTransient(new Exception("mocked error")));
      return mocks;
    }

    static Mocks documentIndexerFailsOnInvalidSynonymDocument(Throwable exception)
        throws Exception {
      Mocks mocks = infiniteDocuments();
      doThrow(exception).when(mocks.documentIndexer).indexDocumentBatch(any());

      return mocks;
    }

    static Mocks mongoClientFails(Throwable exception) throws Exception {
      Mocks mocks = createMocks();

      when(mocks.findCommandMongoClient.hasNext()).thenReturn(true);
      doThrow(exception).when(mocks.findCommandMongoClient).getNext();

      return mocks;
    }
  }
}
