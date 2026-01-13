package com.xgen.testing.mongot.index;

import com.mongodb.client.MongoClient;
import com.xgen.testing.mongot.integration.index.serialization.MongoDbVersionInfo;
import com.xgen.testing.util.MongoDbUtil;
import org.slf4j.Logger;

public class VectorIndexUtils {

  public static final String MIN_MONGODB_VERSION_SUPPORTING_VECTOR_SEARCH = "6.0";

  public static boolean isMongoDbVersionSupported(String testName, Logger log, MongoClient client) {
    String mongoDbVersion = MongoDbUtil.getDbVersion(client);
    if (!new MongoDbVersionInfo(MIN_MONGODB_VERSION_SUPPORTING_VECTOR_SEARCH)
        .isMongoDbVersionSupported(mongoDbVersion)) {
      log.info(
          "test supported for mongodb version 6.0 and higher; "
              + "skipping test {} running on mongodb version {}",
          testName,
          mongoDbVersion);
      return false;
    }

    return true;
  }
}
