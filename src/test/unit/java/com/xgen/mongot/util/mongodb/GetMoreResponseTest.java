package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.serialization.GetMoreResponseProxy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.junit.Assert;
import org.junit.Test;

public class GetMoreResponseTest {

  private static final long CURSOR_ID = 13;
  private static final List<RawBsonDocument> BATCH =
      Collections.singletonList(BsonUtils.documentToRaw(new BsonDocument()));

  private static final BsonDocument POST_BATCH_RESUME_TOKEN =
      new BsonDocument()
          .append("$recordId", new BsonInt64(1))
          .append(
              "$initialSyncId",
              new BsonBinary(
                  UUID.fromString("390a604a-4979-4d10-9f4f-3d2552224aee"),
                  UuidRepresentation.STANDARD));

  @Test
  public void testFromSuccessfulProxy_NoResumeToken() {
    String namespace = "GetMoreResponseTest.testFromSuccessfulProxy_NoResumeToken";
    GetMoreResponseProxy proxy =
        new GetMoreResponseProxy(
            1.0,
            new BsonTimestamp(),
            new BsonDocument(),
            new GetMoreResponseProxy.CursorProxy(namespace, CURSOR_ID, BATCH, null));

    GetMoreResponse response = GetMoreResponse.fromProxy(proxy);
    assertCorrectResponse(response);
    Assert.assertEquals(Optional.empty(), response.getPostBatchResumeToken());
  }

  @Test
  public void testFromSuccessfulProxy_ResumeToken() {
    String namespace = "GetMoreResponseTest.testFromSuccessfulProxy_ResumeToken";
    GetMoreResponseProxy proxy =
        new GetMoreResponseProxy(
            1.0,
            new BsonTimestamp(),
            new BsonDocument(),
            new GetMoreResponseProxy.CursorProxy(
                namespace, CURSOR_ID, BATCH, POST_BATCH_RESUME_TOKEN));

    GetMoreResponse response = GetMoreResponse.fromProxy(proxy);
    assertCorrectResponse(response);
    Assert.assertEquals(POST_BATCH_RESUME_TOKEN, response.getPostBatchResumeToken().get());
  }

  private static void assertCorrectResponse(GetMoreResponse response) {
    Assert.assertEquals(BATCH, response.getBatch());
    Assert.assertEquals(CURSOR_ID, response.getId());
  }
}
