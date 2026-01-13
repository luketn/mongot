package com.xgen.mongot.replication.mongodb.initialsync;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolver;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import org.junit.Assert;
import org.junit.Test;

public class InitialSyncNamespaceChangeCheckTest {

  @Test
  public void testNoExceptionThrownWhenNamespaceHasNotChanged() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    IndexDefinition indexDefinition = SearchIndexDefinitionBuilder.VALID_INDEX;
    MongoNamespace namespace = new MongoNamespace("testDB", "testCollection");
    when(namespaceResolver.isCollectionNameChanged(indexDefinition, "testCollection"))
        .thenReturn(false);
    InitialSyncNamespaceChangeCheck namespaceChangeCheck =
        new InitialSyncNamespaceChangeCheck(namespaceResolver, indexDefinition);
    namespaceChangeCheck.execute(namespace);
  }

  @Test
  public void testExceptionIsThrownOnNamespaceChange() throws Exception {
    NamespaceResolver namespaceResolver = mock(NamespaceResolver.class);
    IndexDefinition indexDefinition = SearchIndexDefinitionBuilder.VALID_INDEX;
    MongoNamespace namespace = new MongoNamespace("testDB", "testCollection");
    when(namespaceResolver.isCollectionNameChanged(indexDefinition, "testCollection"))
        .thenReturn(true);
    InitialSyncNamespaceChangeCheck namespaceChangeCheck =
        new InitialSyncNamespaceChangeCheck(namespaceResolver, indexDefinition);

    InitialSyncException exception =
        Assert.assertThrows(
            InitialSyncException.class, () -> namespaceChangeCheck.execute(namespace));
    Assert.assertEquals("collection name changed", exception.getMessage());
  }
}
