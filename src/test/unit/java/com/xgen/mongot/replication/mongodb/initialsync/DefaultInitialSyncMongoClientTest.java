package com.xgen.mongot.replication.mongodb.initialsync;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.SessionRefresher.mockSessionRefresher;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolutionException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Assert;
import org.junit.Test;

public class DefaultInitialSyncMongoClientTest {

  @Test
  public void testEnsureCollectionNameUnchangedIsUnchanged() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    when(namespaceResolver.isCollectionNameChanged(any(), any())).thenReturn(false);

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mock(MongoClient.class),
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            namespaceResolver,
            "");

    client.ensureCollectionNameUnchanged(MOCK_INDEX_DEFINITION, "foo");
  }

  @Test
  public void testEnsureCollectionNameUnchangedIsChanged() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    when(namespaceResolver.isCollectionNameChanged(any(), any())).thenReturn(true);

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mock(MongoClient.class),
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            namespaceResolver,
            "");

    try {
      client.ensureCollectionNameUnchanged(MOCK_INDEX_DEFINITION, "foo");
      Assert.fail("did not throw InitialSyncException");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isRequiresResync());
    }
  }

  @Test
  public void testEnsureCollectionNameUnchangedThrowsDoesNotExist() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    when(namespaceResolver.isCollectionNameChanged(any(), any()))
        .thenThrow(NamespaceResolutionException.create());

    DefaultInitialSyncMongoClient client =
        new DefaultInitialSyncMongoClient(
            mock(MongoClient.class),
            mockSessionRefresher(),
            new SimpleMeterRegistry(),
            namespaceResolver,
            "");

    try {
      client.ensureCollectionNameUnchanged(MOCK_INDEX_DEFINITION, "foo");
      Assert.fail("did not throw InitialSyncException");
    } catch (InitialSyncException e) {
      Assert.assertTrue(e.isDropped());
    }
  }
}
