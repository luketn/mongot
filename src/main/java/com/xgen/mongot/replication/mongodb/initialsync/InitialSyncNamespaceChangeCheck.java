package com.xgen.mongot.replication.mongodb.initialsync;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.CollectionScanCommandMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.NamespaceResolver;

public class InitialSyncNamespaceChangeCheck
    implements CollectionScanCommandMongoClient.NamespaceChangeCheck<InitialSyncException> {

  private final NamespaceResolver namespaceResolver;
  private final IndexDefinition indexDefinition;

  public InitialSyncNamespaceChangeCheck(
      NamespaceResolver namespaceResolver, IndexDefinition indexDefinition) {
    this.namespaceResolver = namespaceResolver;
    this.indexDefinition = indexDefinition;
  }

  @Override
  public void execute(MongoNamespace namespace) throws InitialSyncException {
    InitialSyncException.wrapIfThrows(
        () -> {
          if (this.namespaceResolver.isCollectionNameChanged(
              this.indexDefinition, namespace.getCollectionName())) {
            throw InitialSyncException.createRequiresResync("collection name changed");
          }
        });
  }
}
