package com.xgen.mongot.util.bson.parser;

import org.bson.BsonValue;

public interface ValueEncoder<T> {

  BsonValue encode(T value);
}
