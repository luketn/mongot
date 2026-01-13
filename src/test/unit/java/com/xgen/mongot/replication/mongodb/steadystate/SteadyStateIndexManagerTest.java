package com.xgen.mongot.replication.mongodb.steadystate;

import static com.google.common.truth.Truth.assertThat;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Assert;
import org.junit.Test;

public class SteadyStateIndexManagerTest {

  private static final ChangeStreamResumeInfo RESUME_INFO_1 =
      ChangeStreamResumeInfo.create(
          new MongoNamespace("database", "collection"),
          new BsonDocument("resumeToken", new BsonInt32(1)));

  private static final ChangeStreamResumeInfo RESUME_INFO_2 =
      ChangeStreamResumeInfo.create(
          new MongoNamespace("database", "collection"),
          new BsonDocument("resumeToken", new BsonInt32(2)));

  @Test
  public void testGetResumeInfoInvokesSupplier() {
    AtomicBoolean first = new AtomicBoolean(true);
    Supplier<ChangeStreamResumeInfo> supplier =
        () -> first.getAndSet(false) ? RESUME_INFO_1 : RESUME_INFO_2;

    CompletableFuture<Void> changeStreamFuture = new CompletableFuture<>();

    SteadyStateIndexManager manager = SteadyStateIndexManager.create(supplier, changeStreamFuture);

    Assert.assertEquals(RESUME_INFO_1, manager.getResumeInfo());
    Assert.assertEquals(RESUME_INFO_2, manager.getResumeInfo());
    Assert.assertEquals(RESUME_INFO_2, manager.getResumeInfo());
  }

  @Test
  public void testChangeStreamFutureFails() throws Exception {
    CompletableFuture<Void> changeStreamFuture = new CompletableFuture<>();

    SteadyStateIndexManager manager =
        SteadyStateIndexManager.create(() -> RESUME_INFO_1, changeStreamFuture);
    CompletableFuture<Void> managerFuture = manager.getFuture();

    Assert.assertFalse(managerFuture.isDone());
    changeStreamFuture.completeExceptionally(SteadyStateException.createDropped());
    assertCompletesDropped(managerFuture);
  }

  private static void assertCompletesDropped(CompletableFuture<?> future) throws Exception {
    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(SteadyStateException.class);
              Assert.assertEquals(
                  SteadyStateException.Type.DROPPED, ((SteadyStateException) throwable).getType());
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testChangeStreamFutureExceeded() throws Exception {
    CompletableFuture<Void> changeStreamFuture = new CompletableFuture<>();

    SteadyStateIndexManager manager =
        SteadyStateIndexManager.create(() -> RESUME_INFO_1, changeStreamFuture);
    CompletableFuture<Void> managerFuture = manager.getFuture();

    Assert.assertFalse(managerFuture.isDone());
    changeStreamFuture.completeExceptionally(SteadyStateException.createFieldExceeded("reason"));
    assertCompletesExceeded(managerFuture);
  }

  private void assertCompletesExceeded(CompletableFuture<Void> future) throws Exception {
    future
        .handle(
            (ignored, throwable) -> {
              Assert.assertNull(ignored);
              Assert.assertNotNull(throwable);
              assertThat(throwable).isInstanceOf(SteadyStateException.class);
              Assert.assertEquals(
                  SteadyStateException.Type.FIELD_EXCEEDED,
                  ((SteadyStateException) throwable).getType());
              return null;
            })
        .get(5, TimeUnit.SECONDS);
  }
}
