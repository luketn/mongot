package com.xgen.mongot.cursor;

import static com.xgen.testing.TestUtils.downgradeCheckedExceptions;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.commons.lang3.Range;
import org.junit.Assert;
import org.junit.Test;

public class CursorIdSupplierTest {

  @Test
  public void testConcurrentAllocationAlwaysWithinRange() {
    var range = Range.of(13L, 30L);
    var supplier = CursorIdSupplier.fromRange(range);
    var numThreads = 8;
    var numAllocations = (range.getMaximum() - range.getMinimum()) * 10000;
    var barrier = new CountDownLatch(numThreads);

    // Each thread should allocate ids and return any they find that are out of bounds.
    Supplier<OptionalLong> runner =
        () -> {
          barrier.countDown();
          downgradeCheckedExceptions(() -> barrier.await());
          return LongStream.range(0, numAllocations)
              .map(ignored -> supplier.nextId())
              .filter(id -> !range.contains(id))
              .findAny();
        };

    var executor = Executors.newCachedThreadPool();
    var futures =
        IntStream.range(0, numThreads)
            // Dispatch each thread.
            .mapToObj(ignored -> CompletableFuture.supplyAsync(runner, executor))
            // Streams are lazily evaluated, so it's important for us to collect all the futures
            // (aka dispatch all threads) rather than trying to continue to process them in a
            // stream, which may only dispatch a single thread and wait for it to terminate (which
            // it would not, since it blocks on the barrier which requires all threads to be
            // running).
            .collect(Collectors.toList());

    Optional<Long> outOfBoundsId =
        futures.stream()
            // Get the out of bounds id from each thread, if any.
            .map(future -> downgradeCheckedExceptions(() -> future.get()))
            // Find the first out of bounds id allocated by any thread, if any.
            .filter(OptionalLong::isPresent)
            .map(OptionalLong::getAsLong)
            .findAny();

    outOfBoundsId.ifPresent(
        id ->
            Assert.fail(
                String.format("generated id %s which is out of expected range %s", id, range)));
  }

  @Test
  public void testConcurrentAllocationIsUniqueWithinNumberOfCursors() {
    var numThreads = 8;
    var allocationsPerThread = 100;
    // Ensure that we have enough range space for the number of threads.
    var range = Range.of(1L, (long) (numThreads * allocationsPerThread));
    var supplier = CursorIdSupplier.fromRange(range);
    var barrier = new CountDownLatch(numThreads);

    // Each thread should return the list of ids it allocates.
    Supplier<List<Long>> runner =
        () -> {
          barrier.countDown();
          downgradeCheckedExceptions(() -> barrier.await());
          return LongStream.range(0, allocationsPerThread)
              .map(ignored -> supplier.nextId())
              .boxed()
              .collect(Collectors.toList());
        };

    var executor = Executors.newCachedThreadPool();
    var futures =
        IntStream.range(0, numThreads)
            // Dispatch each thread.
            .mapToObj(ignored -> CompletableFuture.supplyAsync(runner, executor))
            // Streams are lazily evaluated, so it's important for us to collect all the futures
            // (aka dispatch all threads) rather than trying to continue to process them in a
            // stream, which may only dispatch a single thread and wait for it to terminate (which
            // it would not, since it blocks on the barrier which requires all threads to be
            // running).
            .collect(Collectors.toList());

    List<Long> ids =
        futures.stream()
            // Get the List<Long> from each thread.
            .map(future -> downgradeCheckedExceptions(() -> future.get()))
            // Flatten the List<List<Long>> into a List<Long>.
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // Ensure that all threads got unique ids.
    var seenIds = new HashSet<Long>();
    for (var id : ids) {
      Assert.assertFalse(String.format("duplicate id %s was allocated", id), seenIds.contains(id));
      seenIds.add(id);
    }
  }

  @Test
  public void testStartingValueChanges() {
    // Note that this test is expected to be flaky every (2^63)-2 runs, which
    // is unlikely enough that we don't try to compensate for it.
    long first = CursorIdSupplier.createDefault().nextId();
    long second = CursorIdSupplier.createDefault().nextId();
    Assert.assertNotEquals(first, second);
  }
}
