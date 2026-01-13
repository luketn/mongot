package com.xgen.mongot.metrics.ftdc;

import org.bson.BsonInt32;

class DocumentType {
  static final int METADATA = 0;
  static final int METRIC_CHUNK = 1;

  static final BsonInt32 METRIC_CHUNK_TYPE = new BsonInt32(METRIC_CHUNK);
  static final BsonInt32 METADATA_TYPE = new BsonInt32(METADATA);
}
