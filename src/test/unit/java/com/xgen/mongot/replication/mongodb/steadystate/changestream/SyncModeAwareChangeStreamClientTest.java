package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.steadystate.changestream.ChangeStreamMongoClientFactory.ChangeStreamClientFactory;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class SyncModeAwareChangeStreamClientTest {

  @Test
  public void testModeAwareClientProxiesRequestAndResponseAsIs() throws Exception {
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            mock(ChangeStreamModeSelector.class),
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    var batch = mock(ChangeStreamBatch.class);
    when(syncClient.getNext()).thenReturn(batch);

    // make sure mode aware client proxies the request and returns response from the wrapped client
    Assert.assertEquals(batch, modeAwareClient.getNext());
  }

  @Test
  public void testModeAwareClientProxiesExceptionFromWrappedClient() throws Exception {
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            mock(ChangeStreamModeSelector.class),
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    // emulate wrapped client getMore() failure
    when(syncClient.getNext()).thenThrow(new RuntimeException("getMore() request failure"));

    // make sure the Future has failed
    Exception ex = Assert.assertThrows(Exception.class, modeAwareClient::getNext);
    Assert.assertEquals("getMore() request failure", ex.getMessage());
  }

  @Test
  public void testModeAwareClientFailureOnWrappedClientRestart() throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);
    when(syncClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            generationId);

    verify(clientFactory).resumeChangeStream(eq(generationId), any());

    // another mode selection should trigger client restart
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    // emulate client restart failure
    when(clientFactory.resumeChangeStream(any(), any()))
        .thenThrow(new RuntimeException("restart failure test"));

    // make sure the Future has failed
    Exception ex = Assert.assertThrows(Exception.class, modeAwareClient::getNext);
    Assert.assertEquals("restart failure test", ex.getMessage());
  }

  @Test
  public void testModeIsSwitchedIfInitialSelectionDoesNotMatchSubsequentSelection()
      throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    var operationFactory = mock(ChangeStreamAggregateOperationFactory.class);
    var indexedOperationTemplate = mock(AggregateOperationTemplate.class);
    when(operationFactory.fromResumeInfo(any(), eq(ChangeStreamMode.INDEXED_FIELDS)))
        .thenReturn(indexedOperationTemplate);
    var allFieldsOperationTemplate = mock(AggregateOperationTemplate.class);
    when(operationFactory.fromResumeInfo(any(), eq(ChangeStreamMode.ALL_FIELDS)))
        .thenReturn(allFieldsOperationTemplate);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    // mode is set at the beginning
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            operationFactory,
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            generationId);

    verify(clientFactory).resumeChangeStream(eq(generationId), eq(indexedOperationTemplate));

    // mode has changed after client creation
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.ALL_FIELDS);

    when(syncClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));
    modeAwareClient.getNext();

    // verify client has restarted
    verify(clientFactory).resumeChangeStream(eq(generationId), eq(allFieldsOperationTemplate));

    // verify the mode is not flipped on subsequent getMore() calls
    modeAwareClient.getNext();
    verifyNoMoreInteractions(clientFactory);
  }

  @Test
  public void testModeIsNotSwitchedIfInitialSelectionMatchSubsequentSelections() throws Exception {
    var generationId = new GenerationId(new ObjectId(), Generation.CURRENT);
    var modeSelector = mock(ChangeStreamModeSelector.class);
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);

    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    // INDEXED_FIELDS mode is selected initially
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            modeSelector,
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            generationId);

    verify(clientFactory).resumeChangeStream(eq(generationId), any());

    // the same mode is selected before next getMore() call
    when(modeSelector.getMode(generationId)).thenReturn(ChangeStreamMode.INDEXED_FIELDS);

    when(syncClient.getNext()).thenReturn(mock(ChangeStreamBatch.class));
    modeAwareClient.getNext();

    // make sure client is not restarted
    verifyNoMoreInteractions(clientFactory);
  }

  @Test
  public void testDefaultClientIsClosed() {
    var clientFactory = mock(ChangeStreamClientFactory.class);
    var syncClient = mock(ChangeStreamMongoClient.class);
    when(clientFactory.resumeChangeStream(any(), any())).thenReturn(syncClient);

    var modeAwareClient =
        new ModeAwareChangeStreamClient(
            mock(ChangeStreamModeSelector.class),
            mock(ChangeStreamAggregateOperationFactory.class),
            clientFactory,
            ChangeStreamResumeInfo.create(
                new MongoNamespace("test", "test"), getResumeToken("initial")),
            new GenerationId(new ObjectId(), Generation.CURRENT));

    modeAwareClient.close();
    verify(syncClient).close();
  }

  private BsonDocument getResumeToken(String token) {
    return new BsonDocument("token", new BsonString(token));
  }
}
