package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.BsonUtils;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

public interface DocumentEncodable extends Encodable {

  @Override
  BsonDocument toBson();

  default RawBsonDocument toRawBson() {
    return new RawBsonDocument(toBson(), BsonUtils.BSON_DOCUMENT_CODEC);
  }
}
