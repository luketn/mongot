package com.xgen.mongot.server.command.management.definition.common;

import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class CommonDefinitions {
  public static final int OK_SUCCESS_CODE = 1;
  public static final BsonDocument OK_RESPONSE =
      new BsonDocument().append("ok", new BsonInt32(OK_SUCCESS_CODE));
  public static final String DEFAULT_INDEX_NAME = "default";
}
