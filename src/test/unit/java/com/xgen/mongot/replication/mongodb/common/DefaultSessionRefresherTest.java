package com.xgen.mongot.replication.mongodb.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;
import com.xgen.mongot.util.concurrent.Executors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

public class DefaultSessionRefresherTest {

  @Test
  public void testDoesNotRefreshIfNoSessions() throws Exception {
    var client = mock(MongoClient.class);
    var database = mock(MongoDatabase.class);

    var latch = new CountDownLatch(1);
    when(database.runCommand(any()))
        .then(
            invocation -> {
              latch.countDown();
              return new Document();
            });

    when(client.getDatabase(any())).thenReturn(database);

    var refresher =
        DefaultSessionRefresher.create(
            new SimpleMeterRegistry(),
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    try {
      // Should not call refreshSessions if no sessions are registered.
      Assert.assertFalse(latch.await(500, TimeUnit.MILLISECONDS));
    } finally {
      refresher.shutdown();
    }
  }

  @Test
  public void testRefreshesWhileSessionRegistered() throws Exception {
    var client = mock(MongoClient.class);
    var database = mock(MongoDatabase.class);

    var latch = new CountDownLatch(1);
    var closed = new AtomicBoolean(false);
    var invocationCount = new AtomicInteger();
    var closedInvocationCount = new AtomicInteger();
    when(database.runCommand(any()))
        .then(
            invocation -> {
              invocationCount.incrementAndGet();
              latch.countDown();
              if (closed.get()) {
                closedInvocationCount.incrementAndGet();
              }
              return new Document();
            });

    when(client.getDatabase(any())).thenReturn(database);

    var refresher =
        DefaultSessionRefresher.create(
            new SimpleMeterRegistry(),
            Executors.singleThreadScheduledExecutor("session-refresh", new SimpleMeterRegistry()),
            client,
            Duration.ofMillis(10));

    try {
      // Should refresh once we register a session.
      var session = mock(ClientSession.class);
      var serverSession = mock(ServerSession.class);
      var sessionId = new BsonDocument().append("id", new BsonObjectId());
      when(serverSession.getIdentifier()).thenReturn(sessionId);
      when(session.getServerSession()).thenReturn(serverSession);

      try (var refreshingSession = refresher.register(session)) {
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      }
      closed.set(true);

      // The RefreshingClientSession should have also closed the ClientSession.
      verify(session).close();

      // Wait on the main thread for a little bit. This will allow the thread pool to potentially
      // schedule a few more refresh attempts. These refresh attempts may or may not invoke the
      // runCommand resulting in an actual RefreshSessionsCommand.
      Thread.sleep(250);
      // Verify that we've issued to the RefreshCommand at least once.
      Assert.assertTrue(invocationCount.get() > 0);
      // When a session is closed, it's session id is removed from the list of sessions to refresh
      // that the refresher maintains. However, the refresher may have already initiated a refresh
      // which will include the session for refresh. However, the next refresh after session.close()
      // has returned will not include it. This means that the number of refresh invocations for a
      // given session after close() has been called is at most 1.
      Assert.assertTrue(closedInvocationCount.get() <= 1);
    } finally {
      refresher.shutdown();
    }
  }
}
