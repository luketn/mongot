package com.xgen.testing.util;

import com.mongodb.client.MongoClient;
import org.bson.Document;

public class MongoDbUtil {

  public static String getDbVersion(MongoClient client) {
    return client
        .getDatabase("util")
        .runCommand(new Document("buildInfo", true))
        .getString("version");
  }
}
