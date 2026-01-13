package com.xgen.mongot.replication.mongodb.common;

import com.google.common.testing.EqualsTester;
import com.mongodb.MongoNamespace;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class ChangeStreamResumeInfoTest {

  private static final MongoNamespace NAMESPACE = new MongoNamespace("database", "collection");
  private static final BsonDocument RESUME_TOKEN = new BsonDocument("foo", new BsonString("bar"));

  @Test
  public void testWithoutOptime() {
    ChangeStreamResumeInfo info = ChangeStreamResumeInfo.create(NAMESPACE, RESUME_TOKEN);

    Assert.assertSame(NAMESPACE, info.getNamespace());
    Assert.assertSame(RESUME_TOKEN, info.getResumeToken());
  }

  @Test
  public void testEqualsAndHashCode() {
    ChangeStreamResumeInfo resumeToken1 = ChangeStreamResumeInfo.create(NAMESPACE, RESUME_TOKEN);
    ChangeStreamResumeInfo resumeToken2 = ChangeStreamResumeInfo.create(NAMESPACE, RESUME_TOKEN);
    ChangeStreamResumeInfo resumeToken3 =
        ChangeStreamResumeInfo.create(new MongoNamespace("database2", "collection2"), RESUME_TOKEN);

    EqualsTester equalsTester = new EqualsTester();
    equalsTester.addEqualityGroup(resumeToken1, resumeToken2);
    equalsTester.addEqualityGroup(resumeToken3);
    equalsTester.testEquals();
  }
}
