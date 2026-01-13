package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoClientSettings;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamAggregateOperationBuilder.AggregateOperationTemplate;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException.Type;
import com.xgen.mongot.util.mongodb.BatchMongoClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class ChangeStreamMongoCursorClientTest {

  @Test
  public void testInvalidBatchField_NullOperationTime() {
    var cursorClient = createBatchCursorMongoClient(null, new BsonDocument());

    SteadyStateException exception =
        Assert.assertThrows(SteadyStateException.class, cursorClient::getNext);

    Assert.assertEquals(Type.TRANSIENT, exception.getType());

    Assert.assertNotNull(exception.getCause());
    Assert.assertTrue(exception.getCause() instanceof ChangeStreamCursorClientException);

    Assert.assertNotNull(exception.getCause().getCause());
    Assert.assertTrue(exception.getCause().getCause() instanceof NullPointerException);

    Assert.assertEquals("getOperationTime", exception.getCause().getCause().getMessage());
  }

  @Test
  public void testInvalidBatchField_NullPostBatchResumeToken() {
    var cursorClient = createBatchCursorMongoClient(new BsonTimestamp(100L), null);

    SteadyStateException exception =
        Assert.assertThrows(SteadyStateException.class, cursorClient::getNext);

    Assert.assertEquals(Type.TRANSIENT, exception.getType());
    Assert.assertNotNull(exception.getCause());
    Assert.assertTrue(exception.getCause() instanceof ChangeStreamCursorClientException);

    Assert.assertNotNull(exception.getCause().getCause());
    Assert.assertTrue(exception.getCause().getCause() instanceof NullPointerException);

    Assert.assertEquals("getPostBatchResumeToken", exception.getCause().getCause().getMessage());
  }

  private ChangeStreamMongoCursorClient<SteadyStateException> createBatchCursorMongoClient(
      BsonTimestamp opTime, BsonDocument resumeToken) {
    BatchMongoClient batchMongoClient = mock(BatchMongoClient.class);
    SessionRefresher sessionRefresher = mock(SessionRefresher.class);
    RefreshingClientSession refreshingClientSession = mock(RefreshingClientSession.class);
    AggregateResponseBatchCursor batchCursor = mock(AggregateResponseBatchCursor.class);

    com.mongodb.client.ClientSession clientSession = mock(com.mongodb.client.ClientSession.class);
    when(batchMongoClient.openSession(any())).thenReturn(clientSession);
    when(sessionRefresher.register(any())).thenReturn(refreshingClientSession);

    when(batchMongoClient.getSettings()).thenReturn(MongoClientSettings.builder().build());
    when(batchMongoClient.openCursor(any(), any())).thenReturn(batchCursor);

    when(batchCursor.getOperationTime()).thenReturn(opTime);
    when(batchCursor.getPostBatchResumeToken()).thenReturn(resumeToken);

    return new ChangeStreamMongoCursorClient<>(
        mock(GenerationId.class),
        batchMongoClient,
        sessionRefresher,
        mock(AggregateOperationTemplate.class),
        new SimpleMeterRegistry(),
        SteadyStateException::wrapIfThrows,
        Optional.empty());
  }
}
