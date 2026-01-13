package com.xgen.mongot.replication.mongodb.synonyms;

import com.xgen.mongot.index.IndexGeneration;
import java.util.Collection;
import java.util.concurrent.Executor;

@FunctionalInterface
public interface SynonymMappingManagerFactory {
  Collection<SynonymMappingManager> create(
      SynonymManager synonymManager, Executor lifecycleExecutor, IndexGeneration indexGeneration);
}
