package com.xgen.mongot.metrics.ftdc;

import java.util.Date;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;

class FtdcTestUtil {
  static void assertMetadata(BsonDocument expected, BsonDocument result) {
    Assert.assertTrue(result.isDateTime("_id"));
    Date resultStart = new Date(result.getDateTime("_id").getValue());
    Date expectedStart = new Date(expected.getDateTime("_id").getValue());
    Assert.assertTrue(DateUtils.isSameDay(expectedStart, resultStart));

    Assert.assertEquals(expected.get("type"), result.get("type"));

    assertMetadataDoc(
        "'doc' field is unequal", expected.getDocument("doc"), result.getDocument("doc"));
  }

  static void assertMetadataDoc(
      String message, BsonDocument expectedBson, BsonDocument resultBson) {
    Assert.assertTrue(resultBson.isDateTime(FtdcMetadata.START_KEY));
    Date resultStart = new Date(resultBson.getDateTime(FtdcMetadata.START_KEY).getValue());
    Date expectedStart = new Date(expectedBson.getDateTime(FtdcMetadata.START_KEY).getValue());
    Assert.assertTrue(DateUtils.isSameDay(expectedStart, resultStart));

    resultBson.remove(FtdcMetadata.START_KEY);
    expectedBson.remove(FtdcMetadata.START_KEY);

    Assert.assertEquals(message, expectedBson, resultBson);
  }

  static BsonDocument defaultMetadata() {
    return new BsonDocument()
        .append("_id", new BsonDateTime(new Date().getTime()))
        .append("type", DocumentType.METADATA_TYPE)
        .append("doc", defaultMetadataDoc());
  }

  static BsonDocument defaultMetadataDoc() {
    return new BsonDocument()
        .append(FtdcMetadata.START_KEY, new BsonDateTime(new Date().getTime()))
        .append(FtdcMetadata.TYPE_KEY, new BsonString("mongot"));
  }
}
