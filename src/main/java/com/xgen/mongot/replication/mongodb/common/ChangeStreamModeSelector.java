package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;

public interface ChangeStreamModeSelector {

  enum ChangeStreamMode {

    /** All existing fields should be requested. */
    ALL_FIELDS,
    /** Only fields required for indexing should be requested. */
    INDEXED_FIELDS;

    public static ChangeStreamMode getDefault() {
      return ALL_FIELDS;
    }
  }

  /**
   * Returns the selected mode at the moment of invocation. Clients must take into account that
   * selection happens asynchronously and the mode can be changed over time.
   */
  ChangeStreamMode getMode(GenerationId generationId);

  /** Register index generation for asynchronous mode selection. */
  void register(GenerationId generationId, IndexDefinition definition, MongoNamespace namespace);

  /** Remove index generation from asynchronous mode selection. */
  void remove(GenerationId generationId);

  void shutdown();
}
