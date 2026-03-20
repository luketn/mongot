package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.InOrder;

public class CustomVectorEngineIndexingWorkSchedulerTest {

  private static final IndexCommitUserData COMMIT_USER_DATA =
      getCommitUserData(new MongoNamespace("db", "collection"), 0);

  private static final IndexMetricsUpdater.IndexingMetricsUpdater IGNORE_METRICS =
      SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);

  @Test
  public void testPrepareBatchIsCalledBeforeIndexing() throws Exception {
    CustomVectorEngineIndexingWorkScheduler scheduler = scheduler();
    DocumentIndexer indexer = indexer();

    ObjectId indexId = new ObjectId();
    RawBsonDocument document =
        BsonUtils.documentToRaw(
            new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
    DocumentEvent insertDocument =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent updateDocument =
        DocumentEvent.createUpdate(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document);
    DocumentEvent deleteDocument = DocumentEvent.createDelete(new BsonInt32(1));

    List<DocumentEvent> batch = List.of(insertDocument, updateDocument, deleteDocument);
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    indexingFuture.get(5, TimeUnit.SECONDS);

    InOrder inOrder = inOrder(indexer);
    inOrder.verify(indexer, timeout(500).times(1)).prepareBatch(batch);
    inOrder.verify(indexer, timeout(500).times(3)).indexDocumentEvent(any());
  }

  private CustomVectorEngineIndexingWorkScheduler scheduler() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    NamedExecutorService executor = Executors.fixedSizeThreadPool("indexing", 2, meterRegistry);
    return CustomVectorEngineIndexingWorkScheduler.create(executor);
  }

  private DocumentIndexer indexer() {
    DocumentIndexer indexer =
        com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer
            .mockDocumentIndexer();
    doAnswer(invocation -> invocation.getArgument(0)).when(indexer).prepareBatch(any());
    return indexer;
  }

  private static IndexCommitUserData getCommitUserData(MongoNamespace namespace, int token) {
    return IndexCommitUserData.createChangeStreamResume(
        ChangeStreamResumeInfo.create(namespace, new BsonDocument("token", new BsonInt32(token))),
        IndexFormatVersion.CURRENT);
  }
}
