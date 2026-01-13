package com.xgen.mongot.server.executors;

import com.xgen.mongot.server.command.Command;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class BulkheadCommandExecutorTest {

  private final BulkheadCommandExecutor executor =
      new BulkheadCommandExecutor(new SimpleMeterRegistry());

  @Test
  public void testSyncExecution() throws ExecutionException, InterruptedException {
    var testThreadName = Thread.currentThread().getName();
    this.executor
        .execute(
            new Command() {
              @Override
              public String name() {
                return "";
              }

              @Override
              public BsonDocument run() {
                Assert.assertEquals(testThreadName, Thread.currentThread().getName());
                return null;
              }

              @Override
              public ExecutionPolicy getExecutionPolicy() {
                return ExecutionPolicy.SYNC;
              }
            })
        .get();
  }

  @Test(expected = ExecutionException.class)
  public void testSyncExecutionThrowsException() throws ExecutionException, InterruptedException {
    this.executor
        .execute(
            new Command() {
              @Override
              public String name() {
                return "";
              }

              @Override
              public BsonDocument run() {
                throw new UncheckedIOException(new IOException());
              }

              @Override
              public ExecutionPolicy getExecutionPolicy() {
                return ExecutionPolicy.SYNC;
              }
            })
        .get();
  }

  @Test
  public void testAsyncExecution() throws ExecutionException, InterruptedException {
    var testThreadName = Thread.currentThread().getName();
    this.executor
        .execute(
            new Command() {
              @Override
              public String name() {
                return "";
              }

              @Override
              public BsonDocument run() {
                Assert.assertNotEquals(testThreadName, Thread.currentThread().getName());
                return null;
              }

              @Override
              public ExecutionPolicy getExecutionPolicy() {
                return ExecutionPolicy.ASYNC;
              }
            })
        .get();
  }

  @Test(expected = ExecutionException.class)
  public void testAsyncExecutionThrowsException() throws ExecutionException, InterruptedException {
    this.executor
        .execute(
            new Command() {
              @Override
              public String name() {
                return "";
              }

              @Override
              public BsonDocument run() {
                throw new UncheckedIOException(new IOException());
              }

              @Override
              public ExecutionPolicy getExecutionPolicy() {
                return ExecutionPolicy.ASYNC;
              }
            })
        .get();
  }

  @Test(expected = RejectedExecutionException.class)
  public void testCloseInternalExecutorService() {
    var closedExecutor = new BulkheadCommandExecutor(new SimpleMeterRegistry());
    closedExecutor.close();
    closedExecutor.execute(
        new Command() {
          @Override
          public String name() {
            return "";
          }

          @Override
          public BsonDocument run() {
            return null;
          }

          @Override
          public ExecutionPolicy getExecutionPolicy() {
            return ExecutionPolicy.ASYNC;
          }
        });
  }
}
