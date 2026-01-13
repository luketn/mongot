package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * ChangeStreamPipelineStageProxy is a proxy for a stage of an aggregation pipeline for a change
 * stream.
 */
public class ChangeStreamPipelineStageProxy implements Bson {

  private static final String STAGE_NAME = "$changeStream";

  private final ChangeStreamPipelineStageOptionsProxy options;

  /** Constructs a ChangeStreamPipelineStageProxy. */
  public ChangeStreamPipelineStageProxy(ChangeStreamPipelineStageOptionsProxy options) {
    Check.argNotNull(options, "options");

    this.options = options;
  }

  public ChangeStreamPipelineStageOptionsProxy getOptions() {
    return this.options;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    Check.argNotNull(documentClass, "documentClass");
    Check.argNotNull(codecRegistry, "codecRegistry");

    return new BsonDocument(STAGE_NAME, this.options.toBsonDocument(documentClass, codecRegistry));
  }
}
