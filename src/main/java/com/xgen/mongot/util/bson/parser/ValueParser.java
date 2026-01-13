package com.xgen.mongot.util.bson.parser;

import org.bson.BsonValue;

public interface ValueParser<T> {

  T parse(BsonParseContext context, BsonValue value) throws BsonParseException;
}
