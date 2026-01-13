package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.serialization.FindCommandResponseProxy;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class FindCommandResponseTest {

  private static final BsonTimestamp OPERATION_TIME = new BsonTimestamp();
  private static final long CURSOR_ID = 13;
  private static final List<RawBsonDocument> BATCH =
      Collections.singletonList(BsonUtils.documentToRaw(new BsonDocument()));

  @Test
  public void testFromUnsuccessfulProxy() {
    String namespace = "FindCommandResponseTest.testFromUnsuccessfulProxy";
    FindCommandResponseProxy proxy =
        new FindCommandResponseProxy(
            0.0,
            OPERATION_TIME,
            new BsonDocument(),
            new FindCommandResponseProxy.CursorProxy(namespace, CURSOR_ID, BATCH));

    try {
      FindCommandResponse.fromProxy(proxy);
      Assert.fail("should throw when ok != 1");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testFromSuccessfulProxy() {
    String namespace = "FindCommandResponseTest.testFromSuccessfulProxy";
    FindCommandResponseProxy proxy =
        new FindCommandResponseProxy(
            1.0,
            OPERATION_TIME,
            new BsonDocument(),
            new FindCommandResponseProxy.CursorProxy(namespace, CURSOR_ID, BATCH));

    FindCommandResponse response = FindCommandResponse.fromProxy(proxy);
    assertCorrectResponse(response);
  }

  private static void assertCorrectResponse(FindCommandResponse response) {
    Assert.assertEquals(OPERATION_TIME, response.getOperationTime());
    Assert.assertEquals(BATCH, response.getBatch());
    Assert.assertEquals(CURSOR_ID, response.getId());
  }
}
