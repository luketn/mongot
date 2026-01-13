package com.xgen.mongot.util.bson.parser;

import org.bson.BsonValue;

public interface Encodable {
  BsonValue toBson();
}
