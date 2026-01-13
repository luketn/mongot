package com.xgen.mongot.replication.mongodb.synonyms;

import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.synonym.SynonymRegistry;

public interface SynonymDocumentIndexerFactory {
  SynonymDocumentIndexer create(SynonymRegistry synonymRegistry, SynonymMappingDefinition indexer);
}
