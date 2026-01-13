package com.xgen.mongot.replication.mongodb.common;

public class UnsupportedMongoDbVersionException extends Exception {

  public static final String UNSUPPORTED_MONGODB_VERSION_FOR_NATURAL_ORDER_MESSAGE =
      "Natural order initial collection scan failed because of unsupported mongodb version";

  public UnsupportedMongoDbVersionException(String message) {
    super(message);
  }
}
