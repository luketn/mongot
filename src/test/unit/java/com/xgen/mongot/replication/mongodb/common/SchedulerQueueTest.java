package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.IndexBatches;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.SchedulerBatch;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.testing.ControlledPriorityBlockingQueue;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class SchedulerQueueTest {
  MeterRegistry registry = new SimpleMeterRegistry();
  MetricsFactory metricsFactory = new MetricsFactory("workScheduler", this.registry);
  private static final double NO_EPSILON = 0.0;

  @Test
  public void testOneBatch() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);

    IndexingSchedulerBatch batch = schedulerBatch(genId, Optional.of(new ObjectId()));
    queue.add(batch);

    Assert.assertFalse(queue.isEmpty());

    // remove from the queue
    Assert.assertEquals(batch, queue.remove());

    Assert.assertFalse(queue.isEmpty());

    queue.finalizeBatch(batch);

    // queue is only empty when all work is removed and finalized or cancelled
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void testOneIndexTwoBatches() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    IndexingSchedulerBatch batchOne = schedulerBatch(genId, Optional.of(attemptId));
    IndexingSchedulerBatch batchTwo = schedulerBatch(genId, Optional.of(attemptId));

    queue.add(batchOne);
    queue.add(batchTwo);

    Assert.assertFalse(queue.isEmpty());

    // remove the first batch from the queue
    Assert.assertEquals(batchOne, queue.remove());

    // second batch should not be removed until the first batch is finalized
    CompletableFuture<SchedulerBatch> removeFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return queue.remove();
              } catch (InterruptedException e) {
                return Check.unreachable();
              }
            });

    Assert.assertThrows(TimeoutException.class, () -> removeFuture.get(1, TimeUnit.SECONDS));

    Assert.assertFalse(queue.isEmpty());

    queue.finalizeBatch(batchOne);

    Assert.assertFalse(queue.isEmpty());

    // now that the first batch has been finalized, the removeFuture should finish
    Assert.assertEquals(batchTwo, removeFuture.get());
  }

  @Test
  public void testMultipleIndexes() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genIdOne = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId genIdTwo = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptIdOne = new ObjectId();
    ObjectId attemptIdTwo = new ObjectId();

    IndexingSchedulerBatch batchOneIndexOne = schedulerBatch(genIdOne, Optional.of(attemptIdOne));
    IndexingSchedulerBatch batchTwoIndexOne = schedulerBatch(genIdOne, Optional.of(attemptIdOne));
    IndexingSchedulerBatch batchThreeIndexTwo = schedulerBatch(genIdTwo, Optional.of(attemptIdTwo));
    IndexingSchedulerBatch batchFourIndexOne = schedulerBatch(genIdOne, Optional.of(attemptIdOne));
    IndexingSchedulerBatch batchFiveIndexTwo = schedulerBatch(genIdTwo, Optional.of(attemptIdTwo));

    queue.add(batchOneIndexOne);
    queue.add(batchTwoIndexOne);
    queue.add(batchThreeIndexTwo);
    queue.add(batchFourIndexOne);
    queue.add(batchFiveIndexTwo);

    // first remove should remove batchOneIndexOne
    Assert.assertEquals(batchOneIndexOne, queue.remove());

    // next remove should remove batchThreeIndexTwo, because batchTwo must
    // wait for batchOne to be finalized because they are on the same index
    Assert.assertEquals(batchThreeIndexTwo, queue.remove());

    CompletableFuture<SchedulerBatch> thirdRemoveFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return queue.remove();
              } catch (InterruptedException e) {
                return Check.unreachable();
              }
            });

    // cannot remove more until one of the two indexes finalizes
    Assert.assertThrows(TimeoutException.class, () -> thirdRemoveFuture.get(1, TimeUnit.SECONDS));

    queue.finalizeBatch(batchOneIndexOne);

    // once the first batch finalizes, the remove can finish, returning
    // the next batch for index one.
    Assert.assertEquals(batchTwoIndexOne, thirdRemoveFuture.get());

    // now finalize both batch to and three without removing anything from the queue.
    queue.finalizeBatch(batchThreeIndexTwo);
    queue.finalizeBatch(batchTwoIndexOne);

    // because it was enqueued first, batch four for index one should the next removed, even though
    // the batch from the index two was finalized first
    Assert.assertEquals(queue.remove(), batchFourIndexOne);

    Assert.assertEquals(queue.remove(), batchFiveIndexTwo);
  }

  @Test
  public void cannotAddDifferentPrioritiesSameIndex() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    IndexingSchedulerBatch batchOne =
        schedulerBatch(
            genId, Optional.of(attemptId), SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM);
    IndexingSchedulerBatch batchTwo =
        schedulerBatch(
            genId, Optional.of(attemptId), SchedulerQueue.Priority.INITIAL_SYNC_CHANGE_STREAM);

    queue.add(batchOne);
    Assert.assertThrows(IllegalStateException.class, () -> queue.add(batchTwo));
  }

  @Test
  public void testPriority() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genIdOne = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId genIdTwo = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptIdOne = new ObjectId();
    ObjectId attemptIdTwo = new ObjectId();

    IndexingSchedulerBatch batchOneLowPriority =
        schedulerBatch(
            genIdOne,
            Optional.of(attemptIdOne),
            SchedulerQueue.Priority.INITIAL_SYNC_COLLECTION_SCAN);
    IndexingSchedulerBatch batchTwoHighPriority =
        schedulerBatch(genIdTwo, Optional.of(attemptIdTwo));
    IndexingSchedulerBatch batchThreeHighPriority =
        schedulerBatch(genIdTwo, Optional.of(attemptIdTwo));

    queue.add(batchOneLowPriority);
    queue.add(batchTwoHighPriority);
    queue.add(batchThreeHighPriority);

    // first remove should remove the second batch, as it is higher priority
    Assert.assertEquals(batchTwoHighPriority, queue.remove());

    // next remove should the first batch.  Even though it is lower priority
    // than the third, the third is blocked until the second batch is finalized
    Assert.assertEquals(batchOneLowPriority, queue.remove());

    CompletableFuture<SchedulerBatch> thirdRemoveFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return queue.remove();
              } catch (InterruptedException e) {
                return Check.unreachable();
              }
            });

    // cannot remove more until the second batch is finalized
    Assert.assertThrows(TimeoutException.class, () -> thirdRemoveFuture.get(1, TimeUnit.SECONDS));

    queue.finalizeBatch(batchTwoHighPriority);

    // once the second batch finalizes, the remove can finish, returning
    // the next batch for index one.
    Assert.assertEquals(batchThreeHighPriority, thirdRemoveFuture.get());
  }

  @Test
  public void testCancelOneIndexNoneInFlight() {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);

    IndexingSchedulerBatch batch = schedulerBatch(genId, Optional.of(new ObjectId()));

    Assert.assertTrue(queue.isEmpty());

    queue.add(batch);

    Assert.assertFalse(queue.isEmpty());

    queue.cancel(genId, Optional.of(new ObjectId()), new RuntimeException());

    Assert.assertTrue(queue.isEmpty());

    // since the batch was not in flight, the future should be completed with the
    // cancel reason.
    Assert.assertTrue(batch.future.isCompletedExceptionally());
  }

  @Test
  public void testCancelOneIndexOneInFlight() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    IndexingSchedulerBatch batchOne = schedulerBatch(genId, Optional.of(attemptId));
    IndexingSchedulerBatch batchTwo = schedulerBatch(genId, Optional.of(attemptId));

    Assert.assertTrue(queue.isEmpty());

    queue.add(batchOne);
    queue.add(batchTwo);

    // first batch is now "in flight"
    queue.remove();

    Assert.assertFalse(queue.isEmpty());

    // should only complete the future for the second batch, since the first is in flight
    queue.cancel(genId, Optional.of(new ObjectId()), new RuntimeException());

    Assert.assertTrue(queue.isEmpty());

    // the first batch should not be complete yet
    Assert.assertThrows(TimeoutException.class, () -> batchOne.future.get(5, TimeUnit.SECONDS));

    // the second batch should be completed exceptionally
    Assert.assertTrue(batchTwo.future.isCompletedExceptionally());
  }

  @Test
  public void testCancelMultipleIndexes() throws Exception {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);

    GenerationId genIdOne = new GenerationId(new ObjectId(), Generation.CURRENT);
    GenerationId genIdTwo = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptIdOne = new ObjectId();
    ObjectId attemptIdTwo = new ObjectId();

    IndexingSchedulerBatch batchOne = schedulerBatch(genIdOne, Optional.of(attemptIdOne));
    IndexingSchedulerBatch batchTwo = schedulerBatch(genIdTwo, Optional.of(attemptIdTwo));

    queue.add(batchOne);
    queue.add(batchTwo);

    Assert.assertFalse(queue.isEmpty());

    // should only cancel the first batch
    queue.cancel(genIdOne, Optional.of(new ObjectId()), new RuntimeException());

    Assert.assertFalse(queue.isEmpty());

    Assert.assertEquals(batchTwo, queue.remove());

    queue.finalizeBatch(batchTwo);

    // after finalizing the second batch, the queue should be empty, as the first
    // was cancelled
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void testCancelDuringRemove() throws Exception {
    ControlledPriorityBlockingQueue<IndexBatches<IndexingSchedulerBatch>> priorityQueue =
        ControlledPriorityBlockingQueue.paused();
    SchedulerQueue<IndexingSchedulerBatch> queue =
        new SchedulerQueue<>(priorityQueue, this.metricsFactory);
    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    IndexingSchedulerBatch batch = schedulerBatch(genId, Optional.of(new ObjectId()));
    queue.add(batch);

    // remove from the queue, but block before we call next() on the IndexBatches
    CompletableFuture<SchedulerBatch> takeFuture =
        FutureUtils.checkedSupplyAsync(
            queue::remove, Executors.newSingleThreadExecutor(), InterruptedException.class);

    priorityQueue.waitForNTakes(1);

    // cancel, removing the outstanding batch from the IndexBatches
    queue.cancel(genId, Optional.of(new ObjectId()), new RuntimeException());

    // allow the queue to call next() on the IndexBatches
    priorityQueue.resume();

    // the queue should see that the batches is empty and return to wait for another batch
    Assert.assertThrows(TimeoutException.class, () -> takeFuture.get(5, TimeUnit.SECONDS));

    // if we add another batch, the queue should now return it
    queue.add(batch);
    Assert.assertEquals(batch, takeFuture.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testSubsequentCallsToCancelTheSameGeneration() throws InterruptedException {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);
    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    IndexingSchedulerBatch batchOne = schedulerBatch(genId, Optional.of(attemptId));
    IndexingSchedulerBatch batchTwo = schedulerBatch(genId, Optional.of(attemptId));

    Assert.assertTrue(queue.isEmpty());

    queue.add(batchOne);
    queue.add(batchTwo);

    queue.remove();
    Assert.assertFalse(queue.isEmpty());

    CompletableFuture<Void> future1 =
        queue.cancel(genId, Optional.of(new ObjectId()), new RuntimeException("1"));
    CompletableFuture<Void> future2 =
        queue.cancel(genId, Optional.of(new ObjectId()), new RuntimeException("2"));
    Assert.assertSame(future1, future2);
  }

  @Test
  public void testQueueReturnsCompletedFutureAfterCancelWhenInFlightBatchSucceeds()
      throws InterruptedException {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);
    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    IndexingSchedulerBatch batchOne = schedulerBatch(genId, Optional.of(attemptId));
    Assert.assertTrue(queue.isEmpty());
    queue.add(batchOne);

    IndexingSchedulerBatch inFLightBatch = queue.remove();
    Assert.assertFalse(queue.isEmpty());

    CompletableFuture<Void> preCompleteFuture =
        queue.cancel(genId, Optional.of(attemptId), new RuntimeException());
    Assert.assertSame(preCompleteFuture, inFLightBatch.future);
    Assert.assertFalse(inFLightBatch.future.isDone());

    // complete normally
    inFLightBatch.future.complete(null);

    CompletableFuture<Void> postCompleteFuture =
        queue.cancel(genId, Optional.of(attemptId), new RuntimeException());
    Assert.assertSame(preCompleteFuture, postCompleteFuture);

    queue.finalizeBatch(inFLightBatch);

    CompletableFuture<Void> postFinalizeFuture =
        queue.cancel(genId, Optional.of(attemptId), new RuntimeException());
    Assert.assertNotSame(postCompleteFuture, postFinalizeFuture);

    Assert.assertTrue(postFinalizeFuture.isDone());
    Assert.assertFalse(postFinalizeFuture.isCompletedExceptionally());
  }

  @Test
  public void testQueueReturnsCompletedFutureAfterCancelWhenInFlightBatchFails()
      throws InterruptedException {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);
    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();

    IndexingSchedulerBatch batchOne = schedulerBatch(genId, Optional.of(attemptId));
    Assert.assertTrue(queue.isEmpty());
    queue.add(batchOne);

    IndexingSchedulerBatch inFLightBatch = queue.remove();
    Assert.assertFalse(queue.isEmpty());

    CompletableFuture<Void> preFailureFuture =
        queue.cancel(genId, Optional.of(attemptId), new RuntimeException());
    Assert.assertSame(preFailureFuture, inFLightBatch.future);
    Assert.assertFalse(inFLightBatch.future.isDone());

    inFLightBatch.future.completeExceptionally(new RuntimeException());

    CompletableFuture<Void> postFailureFuture =
        queue.cancel(genId, Optional.of(attemptId), new RuntimeException());
    Assert.assertTrue(postFailureFuture.isDone());
    // verifies that queue invalidated the in-flight future after its failure
    Assert.assertNotSame(postFailureFuture, inFLightBatch.future);
  }

  @Test
  public void testMetricSanity() throws InterruptedException {
    SchedulerQueue<IndexingSchedulerBatch> queue = new SchedulerQueue<>(this.metricsFactory);
    GenerationId genId = new GenerationId(new ObjectId(), Generation.CURRENT);
    ObjectId attemptId = new ObjectId();
    IndexingSchedulerBatch batchOne = schedulerBatch(genId, Optional.of(attemptId));
    IndexingSchedulerBatch batchTwo = schedulerBatch(genId, Optional.of(attemptId));
    IndexingSchedulerBatch batchThree = schedulerBatch(genId, Optional.of(attemptId));
    Assert.assertTrue(queue.isEmpty());
    queue.add(batchOne);
    queue.add(batchTwo);
    queue.add(batchThree);

    // Initially there are two batches in the queue
    Assert.assertEquals(
        3.0,
        this.registry.find("workScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);

    IndexingSchedulerBatch inFLightBatch = queue.remove();
    // Moving the batch to the in-flight status does not alter the queue size because
    // the batch is still considered part of the queue.
    Assert.assertEquals(
        3.0,
        this.registry.find("workScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);

    queue.cancel(genId, Optional.of(new ObjectId()), new RuntimeException());
    // When canceling batches for a specific genId,
    // update the queue size only for batches that are not in-flight.
    Assert.assertEquals(
        1.0,
        this.registry.find("workScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);

    queue.finalizeBatch(inFLightBatch);
    // When an in-flight batch is finalized, the queue size is updated to reflect the change.
    Assert.assertEquals(
        0.0,
        this.registry.find("workScheduler" + ".queuedBatchesTotal").gauge().value(),
        NO_EPSILON);
  }

  private IndexingSchedulerBatch schedulerBatch(
      GenerationId genId, Optional<ObjectId> attemptId, SchedulerQueue.Priority priority) {
    return new IndexingSchedulerBatch(
        List.of(),
        priority,
        DocumentIndexer.mockDocumentIndexer(),
        new CompletableFuture<>(),
        genId,
        attemptId,
        Optional.empty(),
        SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH));
  }

  private IndexingSchedulerBatch schedulerBatch(GenerationId genId, Optional<ObjectId> attemptId) {
    return schedulerBatch(genId, attemptId, SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM);
  }
}
