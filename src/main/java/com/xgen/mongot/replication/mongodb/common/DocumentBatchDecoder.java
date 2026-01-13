package com.xgen.mongot.replication.mongodb.common;

import java.util.List;
import org.bson.RawBsonDocument;

public interface DocumentBatchDecoder {

  void decode(List<RawBsonDocument> events);
}
